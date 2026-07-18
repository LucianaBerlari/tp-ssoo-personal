#include <utils/utils.h>
#include "kernel_scheduler.h"

void liberar_mutex_de_proceso_saliente(t_pcb *pcb_saliente);
void *escuchar_puerto_interrupt(void *args);

void verificar_desalojo_por_prioridad(t_pcb *proceso_entrante);
void *hilo_temporizador_suspension(void *arg);
void intentar_desuspender_procesos();

bool preemption_habilitada = false;
int cantidad_colas_configuradas = 0;
t_list *colas_ready[MAX_PRIORIDADES];
t_algoritmo algoritmos_de_colas[MAX_PRIORIDADES];

// Agregamos como variable global al inicio del .c
uint32_t proximo_pid = 1;
// DEclaramos las salas de espera
t_list *cola_new; // Procesos recien creados, esperando entrar al sistema
// t_list *cola_ready; // Procesos listos para ejecutarse
t_list *cola_exit; // Procesos que terminaron

t_list *cola_block; // Procesos esperando IO
t_list *lista_cpus; // CPUs conectadas

pthread_mutex_t mx_cola_ready; // Protege la lista para que no la toquen dos a la vez
pthread_mutex_t mx_cola_new;   // El candado para la lista de procesos nuevos
// Declara las var globales
pthread_mutex_t mx_cola_block;
pthread_mutex_t mx_lista_cpus;

sem_t sem_procesos_ready; // Avisa cuantos procesos hay listos para ejecutar
sem_t sem_procesos_new;   // Este es el "timbre" de la cola NEW

sem_t sem_cpus_libres; // Cuántas CPUs libres hay disponibles

int socket_memoria;
int socket_io_sleep = -1;
int socket_io_stdout = -1;
int socket_io_stdin = -1;
// int socket_io_global = -1;
int socket_cpu_interrupt = -1;

int ultima_respuesta_km = -1;

// Configuración
t_algoritmo algoritmo_actual;
int quantum_ms; // Solo para RR

t_log *logger;

bool compactacion_en_progreso = false;

// APUNTE: Agregamos la cola para procesos suspendidos y el timeout
t_list *cola_susp_block;
pthread_mutex_t mx_cola_susp_block;
int suspension_timeout_ms; // Aca cargamos el SUSPENSION_TIMEOUT del config

// Estructura para pasarle datos al hilo de la suspension
typedef struct
{
    uint32_t pid;
    int tiempo_bloqueado_ms;
} t_suspension_args;

// Esta estructura la usás para pasarle datos al hilo del quantum
typedef struct
{
    int socket_cpu;       // A qué CPU mandarle la interrupción
    uint32_t pid;         // Qué proceso está corriendo
    int *quantum_vigente; // ← puntero al campo de la cpu
} t_datos_quantum;

// Comunicación con KM
// sem_t sem_ok_carga_proceso; // probando para el problema de los hilos INIT_PROC
/*pthread_mutex_t mx_comunicacion_memoria = PTHREAD_MUTEX_INITIALIZER; // Lo usamos para que KM reciba un mensaje a la vez
sem_t sem_respuesta_memoria; // Lo usamos para esperar la confirmación de KM */

int main(int argc, char *argv[])
{

    saludar("kernel_scheduler");

    // Primero cargamos el config para poder leer el LOG_LEVEL
    t_config *config = config_create(argv[1]);
    if (config == NULL)
    {
        printf("No se pudo cargar el config en la ruta: %s\n", argv[1]);
        exit(EXIT_FAILURE);
    }

    // Log level desde el config
    char *log_level_str = config_has_property(config, "LOG_LEVEL") ? config_get_string_value(config, "LOG_LEVEL") : "DEBUG";
    logger = log_create("kernel_scheduler.log", "Kernel scheduler", 1, log_level_from_string(log_level_str));

    if (config_has_property(config, "PLANIFICATION_ALGORITHM"))
    {
        char *algo = config_get_string_value(config, "PLANIFICATION_ALGORITHM");
        if (strcmp(algo, "FIFO") == 0)
            algoritmo_actual = FIFO;
        else if (strcmp(algo, "RR") == 0)
        {
            algoritmo_actual = RR;
            quantum_ms = config_get_int_value(config, "RR_QUANTUM");
        }
        else
        {
            algoritmo_actual = CMN;
        }

        if (config_has_property(config, "QUEUE_PREEMPTION"))
        {
            char *preemption = config_get_string_value(config, "QUEUE_PREEMPTION");
            preemption_habilitada = (strcmp(preemption, "TRUE") == 0);
        }

        // Cargamos de forma dinamica los algoritmos de cada cola y los GUARDAMOS
        if (config_has_property(config, "QUEUES_ALGORITHMS"))
        {
            char **algos = config_get_array_value(config, "QUEUES_ALGORITHMS");
            int idx = 0;
            while (algos[idx] != NULL)
            {
                if (strcmp(algos[idx], "FIFO") == 0)
                {
                    algoritmos_de_colas[idx] = FIFO;
                }
                else if (strcmp(algos[idx], "RR") == 0)
                {
                    algoritmos_de_colas[idx] = RR;
                }

                free(algos[idx]);
                idx++;
            }
            free(algos);
            cantidad_colas_configuradas = idx;
        }
    }
    else
    {
        log_error(logger, "Falta la clave PLANIFICATION_ALGORITHM en el config!");
        config_destroy(config);
        return EXIT_FAILURE;
    }

    pthread_mutex_init(&mx_cola_ready, NULL); // Indica si hay un hilo actualizando un proceso (les da orden a los hilos)
    sem_init(&sem_procesos_ready, 0, 0);      // Empieza en 0 porque no hay nadie listo

    sem_init(&sem_procesos_new, 0, 0);
    pthread_mutex_init(&mx_cola_new, NULL); // Preparamos el candado para usarlo

    // Estos dos mutex son para la comunicación con KM
    // sem_init(&sem_ok_carga_proceso, 0, 0); //problema hilos init_proc
    sem_init(&sem_respuesta_memoria, 0, 0);
    pthread_mutex_init(&mx_comunicacion_memoria, NULL);

    pthread_mutex_init(&mx_cola_block, NULL);
    pthread_mutex_init(&mx_lista_cpus, NULL);

    sem_init(&sem_cpus_libres, 0, 0);

    // Creamos las salas de espera
    cola_new = list_create();
    for (int i = 0; i < MAX_PRIORIDADES; i++)
    {
        colas_ready[i] = list_create();
    }
    cola_exit = list_create();

    // DOnde van los Mutex cuando se creen (SE puede ver como un cajon vacio)
    lista_mutex_globales = list_create();
    pthread_mutex_init(&mx_lista_mutex, NULL);

    cola_block = list_create();
    cola_susp_block = list_create();
    pthread_mutex_init(&mx_cola_susp_block, NULL);
    if (config_has_property(config, "SUSPENSION_TIMEOUT"))
    {
        suspension_timeout_ms = config_get_int_value(config, "SUSPENSION_TIMEOUT");
    }
    lista_cpus = list_create();

    // Parte cliente
    // Conexion a Kernel Memory (cliente)
    char *ip_memoria = config_get_string_value(config, "IP_MEMORIA");
    char *puerto_memoria = config_get_string_value(config, "PUERTO_MEMORIA");

    // probamos enum
    socket_memoria = crear_conexion(ip_memoria, puerto_memoria);

    if (socket_memoria != -1)
    {
        log_info(logger, "¡CONEXION HECHA!");
        log_info(logger, "## Conectado a Kernel Memory");

        int tipo = KERNEL_SCHEDULER;
        send(socket_memoria, &tipo, sizeof(int), 0);

        pthread_t hilo_memoria;
        pthread_create(&hilo_memoria, NULL, escuchar_kernel_memory, NULL);
        pthread_detach(hilo_memoria);

        if (argc > 2)
        {
            char *path_del_script = argv[2]; // El path que pasas por consola

            enviar_creacion_proceso(socket_memoria, 0, path_del_script);

            // pthread_mutex_lock(&mx_recv_mensaje);
            // int respuesta;
            // recv(socket_memoria, &respuesta, sizeof(respuesta), MSG_WAITALL); // Lo agregue para que haya un orden fijo --> 1.crear proceso 2.actualizar contexto 3.eliminar proceso
            // pthread_mutex_unlock(&mx_recv_mensaje);

            if (ultima_respuesta_km == OK)
            {
                log_info(logger, "El proceso inicial (PID 0) se creo correctamente.");
            }
            else if (ultima_respuesta_km == ERROR)
            {
                log_error(logger, "## ERROR CRÍTICO: Kernel Memory notifico error en la creación del proceso inicial (PID 0). Abortando.");
                log_destroy(logger);
                config_destroy(config);
                exit(EXIT_FAILURE); // Frenamos el inicio en seco
            }
            else
            {
                log_error(logger, "## ERROR CRÍTICO: Respuesta erronea o desconocida de Kernel Memory al crear PID 0. Abortando.");
                log_destroy(logger);
                config_destroy(config);
                exit(EXIT_FAILURE); // Frenamos el inicio en seco
            }

            // ------------------------

            t_pcb *pcb_inicial = crear_pcb(0, 0); // PID 0, prioridad 0

            pthread_mutex_lock(&mx_cola_new);
            list_add(cola_new, pcb_inicial);
            pthread_mutex_unlock(&mx_cola_new);

            log_info(logger, "## (%d) Se crea el proceso - Estado: NEW", pcb_inicial->pid);

            sem_post(&sem_procesos_new);
        }
    }
    else
    {
        log_error(logger, "No se pudo conectar a Kernel Memory");
        return EXIT_FAILURE;
    }
    /*int conexion_memoria = crear_conexion(ip_memoria, puerto_memoria);
    if (conexion_memoria == -1)
    {
        log_error(logger, "No se pudo conectar a Kernel Memory");
        return 1;
    }*/

    pthread_t hilo_largo_plazo;
    pthread_create(&hilo_largo_plazo, NULL, planificador_largo_plazo, NULL);
    pthread_detach(hilo_largo_plazo);

    pthread_t hilo_corto_plazo;
    pthread_create(&hilo_corto_plazo, NULL, planificador_corto_plazo, NULL);
    pthread_detach(hilo_corto_plazo);

    /*pthread_t hilo_memoria;
    pthread_create(&hilo_memoria, NULL, escuchar_kernel_memory, NULL);
    pthread_detach(hilo_memoria);*/

    pthread_t hilo_interrupt;
    pthread_create(&hilo_interrupt, NULL, escuchar_puerto_interrupt, NULL);
    pthread_detach(hilo_interrupt);

    // Parte del servidor
    // Incializo
    char *puerto = config_get_string_value(config, "PUERTO_KERNEL_SCHEDULER");
    int server_kernel_scheduler = iniciar_servidor(puerto);

    // Parte servidor
    log_info(logger, "Servidor listo para recibir al cliente");
    while (1)
    {

        int cliente_fd = esperar_cliente(server_kernel_scheduler);

        if (cliente_fd < 0)
        {
            log_error(logger, "Error al aceptar cliente");
            continue;
        }

        log_info(logger, "Se conectó un cliente");

        pthread_t thread;

        int *fd_ptr = malloc(sizeof(int));
        *fd_ptr = cliente_fd;

        pthread_create(&thread, NULL, atender_cliente, fd_ptr);
        pthread_detach(thread);
    }

    /*int cliente_io = esperar_cliente(server_kernel_scheduler);
    int cliente_CPU = esperar_cliente(server_kernel_scheduler);
    // chequeo que inciar_servidor me haya dado algo valido
    if (server_kernel_scheduler < 0)
    {
        log_error(logger, "error en inciar servidor");
        config_destroy(config);
        exit(EXIT_FAILURE);
    }
    else
    {
        recibir_mensaje(cliente_io);
    }
    if (cliente_CPU < 0)
    {
        log_error(logger, "Error en esperar cliente de:%n", cliente_CPU);
        exit(EXIT_FAILURE);
    }
    if (cliente_CPU < 0)
    {
        log_error(logger, "Error en esperar cliente de:%n", cliente_CPU);
        exit(EXIT_FAILURE);
    }
    else
    {
        recibir_mensaje(cliente_CPU);
    }*/

    // Limpieza
    log_destroy(logger);
    config_destroy(config);
}
// función auxiliar luego del main y antes de iteratorr y planificacion
char *estado_a_string(t_estado e)
{
    switch (e)
    {
    case NEW:
        return "NEW";
    case READY:
        return "READY";
    case EXEC:
        return "EXEC";
    case BLOCK:
        return "BLOCK";
    case EXIT:
        return "EXIT";
    default:
        return "DESCONOCIDO";
    }
}

typedef struct
{
    t_pcb *pcb;
    uint32_t tiempo_ms;
} t_sleep_args;

void *ejecutar_sleep(void *arg)
{
    t_sleep_args *datos = (t_sleep_args *)arg;
    usleep(datos->tiempo_ms * 1000);

    // Sacar de BLOCK
    pthread_mutex_lock(&mx_cola_block);
    for (int i = 0; i < list_size(cola_block); i++)
    {
        t_pcb *p = list_get(cola_block, i);
        if (p->pid == datos->pcb->pid)
        {
            list_remove(cola_block, i);
            break;
        }
    }
    pthread_mutex_unlock(&mx_cola_block);

    // Mover a READY
    datos->pcb->estado = READY;
    log_info(logger, "## (%d) finalizo IO y pasa a READY", datos->pcb->pid);

    pthread_mutex_lock(&mx_cola_ready);
    list_add(colas_ready[datos->pcb->prioridad], datos->pcb);
    pthread_mutex_unlock(&mx_cola_ready);

    // Verificamos si el proceso que sale de la IO desaloja a alguien en CPU
    verificar_desalojo_por_prioridad(datos->pcb);

    sem_post(&sem_procesos_ready);
    free(datos);
    return NULL;
}

void iteratorr(char *value)
{
    log_info(logger, "%s", value);
};

void *atender_cliente(void *arg)
{
    // EL hilo recibe un puntero, lo primero que hago es guarda el valor
    int socket_cliente = *(int *)arg;
    free(arg);
    // Recibimos al tipo de cliente
    int tipo;
    recv(socket_cliente, &tipo, sizeof(int), MSG_WAITALL);

    switch (tipo)
    {

    case IO:

        int tipo_io;
        recv(socket_cliente, &tipo_io, sizeof(int), MSG_WAITALL);
        if (tipo_io == STDOUT)
        {
            log_info(logger, "Se conectó IO de tipo STDOUT");
            socket_io_stdout = socket_cliente;
            log_info(logger, "Socket de IO STDOUT registrado en FD: %d", socket_io_stdout);
        }
        else if (tipo_io == SLEEP)
        {
            log_info(logger, "Se conectó IO de tipo SLEEP");
            socket_io_sleep = socket_cliente;
            log_info(logger, "Socket de IO SLEEP registrado en FD: %d", socket_io_sleep);
        }
        else if (tipo_io == STDIN)
        {
            log_info(logger, "Se conectó IO de tipo STDIN");
            socket_io_stdin = socket_cliente;
            log_info(logger, "Socket de IO STDIN registrado en FD: %d", socket_io_stdin);
        }
        else
        {
            log_info(logger, "Se conectó IO de tipo desconocida");
            close(socket_cliente);
        }

        // socket_io_global = socket_cliente; // <--guardamos el socket para usarlo en las syscalls

        break;

    case CPU:
        log_info(logger, "Se conecto una CPU - Iniciando Handshake");

        // 1. Enviamos el codigo de operacion PEDIR_ID_CPU
        op_code op_pedido = PEDIR_ID_CPU;
        send(socket_cliente, &op_pedido, sizeof(op_code), 0);

        // 2. Preparamos variables para recibir la respuesta
        op_code op_respuesta;
        int32_t id_real_cpu;

        if (recv(socket_cliente, &op_respuesta, sizeof(op_code), MSG_WAITALL) <= 0)
        {
            log_error(logger, "Error al recibir codigo de respuesta de la CPU");
            close(socket_cliente);
            break;
        }

        if (recv(socket_cliente, &id_real_cpu, sizeof(int32_t), MSG_WAITALL) <= 0)
        {
            log_error(logger, "Error al recibir el valor del ID de la CPU");
            close(socket_cliente);
            break;
        }

        // 3. Creamos la estructura asignando el socket de interrupción actual
        t_cpu_info *nueva_cpu = malloc(sizeof(t_cpu_info));
        nueva_cpu->socket_dispatch = socket_cliente;

        // Sincronizamos con el socket que acaba de aceptar el hilo de interrupciones
        nueva_cpu->socket_interrupt = socket_cpu_interrupt;
        nueva_cpu->cpu_id = id_real_cpu;
        nueva_cpu->ocupada = 0;
        nueva_cpu->pcb_ejecutando = NULL;
        nueva_cpu->quantum_vigente = 0; // para Round Robin
        // Guardo la cpu en la lista
        pthread_mutex_lock(&mx_lista_cpus);
        list_add(lista_cpus, nueva_cpu);
        pthread_mutex_unlock(&mx_lista_cpus);
        // Incremento las cpus libres
        sem_post(&sem_cpus_libres);
        log_info(logger, "## CPU %d conectada y registrada con h_interrupt: %d", id_real_cpu, nueva_cpu->socket_interrupt);
        break;

    default:
        log_warning(logger, "Cliente desconocido");
        close(socket_cliente);
        return NULL;
    }

    while (1)
    {

        int cod_op = recibir_operacion(socket_cliente);

        if (cod_op == -1)
        {
            log_warning(logger, "Cliente desconectado");
            close(socket_cliente);
            return NULL;
        }
        // Ahora escuchamos las operaciones mandadas x el cliente
        switch (cod_op)
        {

        case MENSAJE:
            recibir_mensaje(socket_cliente);
            break;

            // el switch la atrapa y ejecuta la funcion correspondiente.
        // Cpu lo envia cuando finaliza un proceso
        case MOTIVO_EXIT:
        {
            // Recibo el paquete con el PID
            t_list *datos = recibir_paquete(socket_cliente);
            int pid = *(int *)list_get(datos, 0);
            list_destroy_and_destroy_elements(datos, free);
            // Busco el pcb que ejecuto esa cpu
            t_pcb *pcb = buscar_pcb_en_ejecucion(socket_cliente);
            if (pcb == NULL)
                break;
            // El proceso finalizo correctamente
            log_info(logger, "## (%d) finalizo su ejecucion con motivo de SUCCESS", pid);

            // Liberar mutex que tenía
            liberar_mutex_de_proceso_saliente(pcb);

            // Avisar a memoria
            enviar_finalizacion_proceso(socket_memoria, pid);

            // Marcar CPU libre
            pthread_mutex_lock(&mx_lista_cpus);
            for (int i = 0; i < list_size(lista_cpus); i++)
            {
                t_cpu_info *cpu = list_get(lista_cpus, i);
                if (cpu->socket_dispatch == socket_cliente)
                {
                    cpu->ocupada = 0;
                    cpu->pcb_ejecutando = NULL;
                    cpu->quantum_vigente = 0;
                    break;
                }
            }
            pthread_mutex_unlock(&mx_lista_cpus);

            pcb->estado = EXIT;
            list_add(cola_exit, pcb);
            sem_post(&sem_cpus_libres);
            break;
        }
        // Cpu lo envia cuando intenta acceder a memoria invalida
        case MOTIVO_SEG_FAULT:
        {
            // Recibo el paquete con el PID
            t_list *datos = recibir_paquete(socket_cliente);
            int pid = *(int *)list_get(datos, 0);
            list_destroy_and_destroy_elements(datos, free);
            // Como antes, busco el pcb que ejecutaba esa cpu
            t_pcb *pcb = buscar_pcb_en_ejecucion(socket_cliente);
            if (pcb == NULL)
                break;
            // Finaliza por error de memoria
            log_info(logger, "## (%d) finalizo su ejecucion con motivo de SEGMENTATION FAULT", pid);

            // Liberar mutex que tenía
            liberar_mutex_de_proceso_saliente(pcb);

            // Avisar a memoria
            enviar_finalizacion_proceso(socket_memoria, pid);
            /// sem_wait(&respuesta_memoria);
            // Marcar CPU libre
            pthread_mutex_lock(&mx_lista_cpus);
            for (int i = 0; i < list_size(lista_cpus); i++)
            {
                t_cpu_info *cpu = list_get(lista_cpus, i);
                if (cpu->socket_dispatch == socket_cliente)
                {
                    cpu->ocupada = 0;
                    cpu->pcb_ejecutando = NULL;
                    cpu->quantum_vigente = 0;
                    break;
                }
            }
            pthread_mutex_unlock(&mx_lista_cpus);

            pcb->estado = EXIT;
            list_add(cola_exit, pcb);
            sem_post(&sem_cpus_libres);
            break;
        }
        case MOTIVO_SYSCALL:
        {
            // Recibo el paquete completo
            t_list *datos = recibir_paquete(socket_cliente);
            int pid = *(int *)list_get(datos, 0);
            int tipo_syscall = *(int *)list_get(datos, 1);

            t_pcb *pcb = buscar_pcb_en_ejecucion(socket_cliente);
            if (pcb == NULL)
            {
                list_destroy_and_destroy_elements(datos, free);
                break;
            }

            log_info(logger, "## (%d) - Solicitó syscall: %d", pid, tipo_syscall);
            // Recibo el tipo de syscall y trabajo respecto a la misma
            switch (tipo_syscall)
            {
            case S_STDOUT:
            {
                // Cpu envia donde esta guardado en memoria lo que se quiere imprimir
                u_int32_t dir_logica = *(u_int32_t *)list_get(datos, 2);
                u_int32_t tamanio = *(u_int32_t *)list_get(datos, 3);

                t_estado ant = pcb->estado;
                pcb->estado = BLOCK;
                log_info(logger, "## (%d) Pasa del estado %s al estado BLOCK [STDOUT]",
                         pcb->pid, estado_a_string(ant));

                pthread_mutex_lock(&mx_cola_block);
                list_add(cola_block, pcb);
                pthread_mutex_unlock(&mx_cola_block);

                // Temporizador de suspensión
                t_suspension_args *args_stdout = malloc(sizeof(t_suspension_args));
                args_stdout->pid = pcb->pid;
                args_stdout->tiempo_bloqueado_ms = suspension_timeout_ms;
                pthread_t t_susp_stdout;
                pthread_create(&t_susp_stdout, NULL, hilo_temporizador_suspension, args_stdout);
                pthread_detach(t_susp_stdout);

                // Liberar CPU ya que mientras imprime no necesito CPU
                pthread_mutex_lock(&mx_lista_cpus);
                for (int i = 0; i < list_size(lista_cpus); i++)
                {
                    t_cpu_info *cpu = list_get(lista_cpus, i);
                    if (cpu->socket_dispatch == socket_cliente)
                    {
                        cpu->ocupada = 0;
                        cpu->pcb_ejecutando = NULL;
                        cpu->quantum_vigente = 0;
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_lista_cpus);
                // Para despertar al planificador
                sem_post(&sem_cpus_libres);

                // Pedir los datos a KM antes de mandar a IO
                pthread_mutex_lock(&mx_comunicacion_memoria);

                t_paquete *p_lectura = crear_paquete(SOLICITUD_LECTURA);
                agregar_a_paquete(p_lectura, &pcb->pid, sizeof(uint32_t));
                agregar_a_paquete(p_lectura, &tamanio, sizeof(uint32_t));
                agregar_a_paquete(p_lectura, &dir_logica, sizeof(uint32_t));
                enviar_paquete(p_lectura, socket_memoria);
                eliminar_paquete(p_lectura);

                // Recibir respuesta de KM con los datos
                int cod_resp = recibir_operacion(socket_memoria);
                if (cod_resp == RESPUESTA_LECTURA)
                {
                    t_list *datos_leidos = recibir_paquete(socket_memoria);
                    void *contenido = list_get(datos_leidos, 0);

                    // Ahora sí mandamos a IO con los datos reales
                    t_paquete *pack_io = crear_paquete(STDOUT);
                    agregar_a_paquete(pack_io, &pcb->pid, sizeof(uint32_t));
                    agregar_a_paquete(pack_io, &tamanio, sizeof(uint32_t));
                    agregar_a_paquete(pack_io, contenido, tamanio);
                    enviar_paquete(pack_io, socket_io_stdout);
                    eliminar_paquete(pack_io);

                    list_destroy_and_destroy_elements(datos_leidos, free);
                }
                else
                {
                    log_error(logger, "Error leyendo memoria para STDOUT del PID %d", pcb->pid);
                }
                // Libero el mutex
                pthread_mutex_unlock(&mx_comunicacion_memoria);
                break;
            }
            case S_STDIN:
            {
                // Cpu envia la dir.logica y la cantidad de bytes y los guardo(Proceso a leer)
                uint32_t dir_logica = *(uint32_t *)list_get(datos, 2);
                uint32_t tamanio = *(uint32_t *)list_get(datos, 3);

                if (tamanio == 0)
                {
                    log_error(logger, "PID: %u -  Se solicito leer 0 caracteres", pid);
                    break;
                }

                t_estado ant = pcb->estado;
                pcb->estado = BLOCK;

                pcb->dir_logica_io = dir_logica;
                pcb->tamanio_io = tamanio;

                log_info(logger, "## (%d) Pasa del estado %s al estado BLOCK [STDIN]", pcb->pid, estado_a_string(ant));

                pthread_mutex_lock(&mx_cola_block);
                list_add(cola_block, pcb);
                pthread_mutex_unlock(&mx_cola_block);

                t_suspension_args *args_stdin = malloc(sizeof(t_suspension_args));
                args_stdin->pid = pcb->pid;
                args_stdin->tiempo_bloqueado_ms = suspension_timeout_ms;

                pthread_t t_susp_stdin;
                pthread_create(&t_susp_stdin, NULL, hilo_temporizador_suspension, args_stdin);
                pthread_detach(t_susp_stdin);

                // Liberar CPU
                pthread_mutex_lock(&mx_lista_cpus);
                for (int i = 0; i < list_size(lista_cpus); i++)
                {
                    t_cpu_info *cpu = list_get(lista_cpus, i);
                    if (cpu->socket_dispatch == socket_cliente)
                    {
                        cpu->ocupada = 0;
                        cpu->pcb_ejecutando = NULL;
                        cpu->quantum_vigente = 0; // APUNTE: Matamos el quantum para que no tire interrupciones fantasma
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_lista_cpus);
                sem_post(&sem_cpus_libres);

                // Enviar el paquete real al proceso IO
                t_paquete *pack_io = crear_paquete(STDIN); // cambio code_op sin S_
                agregar_a_paquete(pack_io, &pcb->pid, sizeof(uint32_t));
                agregar_a_paquete(pack_io, &tamanio, sizeof(uint32_t));
                enviar_paquete(pack_io, socket_io_stdin);
                eliminar_paquete(pack_io);
                break;
            }
            case S_SLEEP:
            {
                // Un proceso lo paso de exec a block -> Io espera y vuelvo a ready
                int tiempo_ms = *(int *)list_get(datos, 2);

                t_estado ant = pcb->estado;
                pcb->estado = BLOCK;
                log_info(logger, "## (%d) Pasa del estado %s al estado BLOCK [SLEEP]", pid, estado_a_string(ant));

                pthread_mutex_lock(&mx_cola_block);
                list_add(cola_block, pcb);
                pthread_mutex_unlock(&mx_cola_block);

                t_suspension_args *args_sleep = malloc(sizeof(t_suspension_args));
                args_sleep->pid = pcb->pid;
                args_sleep->tiempo_bloqueado_ms = suspension_timeout_ms;

                pthread_t t_susp_sleep;
                pthread_create(&t_susp_sleep, NULL, hilo_temporizador_suspension, args_sleep);
                pthread_detach(t_susp_sleep);

                // Liberar CPU
                pthread_mutex_lock(&mx_lista_cpus);
                for (int i = 0; i < list_size(lista_cpus); i++)
                {
                    t_cpu_info *cpu = list_get(lista_cpus, i);
                    if (cpu->socket_dispatch == socket_cliente)
                    {
                        cpu->ocupada = 0;
                        cpu->pcb_ejecutando = NULL;
                        cpu->quantum_vigente = 0; // APUNTE: Matamos el quantum para que no tire interrupciones fantasma
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_lista_cpus);
                sem_post(&sem_cpus_libres);

                // Enviar el paquete real al proceso IO con el tiempo
                t_paquete *pack_io = crear_paquete(SLEEP); // cambio code_op sin S_
                agregar_a_paquete(pack_io, &pcb->pid, sizeof(uint32_t));
                agregar_a_paquete(pack_io, &tiempo_ms, sizeof(uint32_t));
                enviar_paquete(pack_io, socket_io_sleep);
                eliminar_paquete(pack_io);
                break;
            }
            case S_MEM_ALLOC:
            {
                // Proceso pide memoria -> se bloquea -> Km crea el segmento y vuelve a ready
                int id_seg = *(int *)list_get(datos, 2);
                int tam = *(int *)list_get(datos, 3);

                t_estado ant = pcb->estado;
                pcb->estado = BLOCK;
                log_info(logger, "## (%d) Pasa del estado %s al estado BLOCK [MEM_ALLOC]",
                         pid, estado_a_string(ant));

                pthread_mutex_lock(&mx_cola_block);
                list_add(cola_block, pcb);
                pthread_mutex_unlock(&mx_cola_block);

                pthread_mutex_lock(&mx_lista_cpus);
                for (int i = 0; i < list_size(lista_cpus); i++)
                {
                    t_cpu_info *cpu = list_get(lista_cpus, i);
                    if (cpu->socket_dispatch == socket_cliente)
                    {
                        cpu->ocupada = 0;
                        cpu->pcb_ejecutando = NULL;
                        cpu->quantum_vigente = 0;
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_lista_cpus);
                sem_post(&sem_cpus_libres);

                t_paquete *p = crear_paquete(CREAR_SEGMENTO);
                agregar_a_paquete(p, &pid, sizeof(int));
                agregar_a_paquete(p, &id_seg, sizeof(int));
                agregar_a_paquete(p, &tam, sizeof(int));
                enviar_paquete(p, socket_memoria);
                eliminar_paquete(p);
                break;
            }
            // falta probar porque KS no lee la confirmación del segmento creado que envía KM
            case S_MEM_FREE:
            {
                // Elimino un segmento directamente, sin esperar confirmacion
                int id_seg = *(int *)list_get(datos, 2);

                t_paquete *p = crear_paquete(ELIMINAR_SEGMENTO);
                agregar_a_paquete(p, &pid, sizeof(int));
                agregar_a_paquete(p, &id_seg, sizeof(int));
                enviar_paquete(p, socket_memoria);
                eliminar_paquete(p);

                pthread_mutex_lock(&mx_lista_cpus);
                for (int i = 0; i < list_size(lista_cpus); i++)
                {
                    t_cpu_info *cpu = list_get(lista_cpus, i);
                    if (cpu->socket_dispatch == socket_cliente)
                    {
                        cpu->ocupada = 0;
                        cpu->pcb_ejecutando = NULL;
                        cpu->quantum_vigente = 0;
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_lista_cpus);
                sem_post(&sem_cpus_libres);

                pcb->estado = READY;
                pthread_mutex_lock(&mx_cola_ready);
                list_add(colas_ready[pcb->prioridad], pcb);
                pthread_mutex_unlock(&mx_cola_ready);
                sem_post(&sem_procesos_ready);
                break;
            }
            case S_INIT_PROC:
            {
                // Creo un proceso hijo, recibo el archivo y la prioridad
                char *archivo = (char *)list_get(datos, 2);
                uint32_t prioridad = *(uint32_t *)list_get(datos, 3);

                // Todavía NO incrementamos el PID
                uint32_t nuevo_pid = proximo_pid;

                log_info(logger,
                         "## Solicitud INIT_PROC recibida. Solicitando a Memory crear el PID %u",
                         nuevo_pid);

                // Avisar a Kernel Memory
                enviar_creacion_proceso(socket_memoria, nuevo_pid, archivo);

                // El hilo escuchar_kernel_memory va a recibir el OK/ERROR de KM,
                // guardará el resultado en "ultima_respuesta_km" y destraba este semáforo
                //sem_wait(&sem_respuesta_memoria); // volvi a descomentarlo para sincronizar los hilos

                // --- EVALUAMOS LA RESPUESTA DE KERNEL MEMORY ---
                if (ultima_respuesta_km != OK)
                {
                    log_error(logger,
                              "Kernel Memory rechazó la creación del proceso hijo PID %u. No se agregará al sistema.",
                              nuevo_pid);

                    // Como la syscall falló, devolvemos al proceso padre a READY para que continúe su vida
                    t_pcb *pcb_padre = buscar_pcb_en_ejecucion(socket_cliente);
                    if (pcb_padre != NULL)
                    {
                        pthread_mutex_lock(&mx_lista_cpus);
                        for (int i = 0; i < list_size(lista_cpus); i++)
                        {
                            t_cpu_info *cpu = list_get(lista_cpus, i);
                            if (cpu->socket_dispatch == socket_cliente)
                            {
                                cpu->ocupada = 0;
                                cpu->pcb_ejecutando = NULL;
                                cpu->quantum_vigente = 0;
                                break;
                            }
                        }
                        pthread_mutex_unlock(&mx_lista_cpus);

                        sem_post(&sem_cpus_libres);

                        pcb_padre->estado = READY;
                        log_info(logger, "## (%d) Pasa del estado EXEC al estado READY", pcb_padre->pid);

                        pthread_mutex_lock(&mx_cola_ready);
                        list_add(colas_ready[pcb_padre->prioridad], pcb_padre);
                        pthread_mutex_unlock(&mx_cola_ready);

                        sem_post(&sem_procesos_ready);
                    }
                    break; // Salimos de la syscall sin crear el proceso hijo
                }

                // SI LLEGÓ ACÁ ES PORQUE KM DEVOLVIÓ OK
                log_info(logger,
                         "Kernel Memory creó correctamente el proceso PID %u",
                         nuevo_pid);

                // Recién ahora el PID queda reservado de forma segura
                proximo_pid++;

                // Crear PCB nuevo y meterlo en NEW
                t_pcb *nuevo_pcb = crear_pcb(nuevo_pid, prioridad);

                pthread_mutex_lock(&mx_cola_new);
                list_add(cola_new, nuevo_pcb);
                pthread_mutex_unlock(&mx_cola_new);

                log_info(logger,
                         "## (%u) Se crea el proceso - Estado: NEW",
                         nuevo_pid);

                sem_post(&sem_procesos_new);

                // El proceso padre vuelve a READY de forma exitosa
                t_pcb *pcb_padre = buscar_pcb_en_ejecucion(socket_cliente);

                if (pcb_padre != NULL)
                {
                    pthread_mutex_lock(&mx_lista_cpus);
                    for (int i = 0; i < list_size(lista_cpus); i++)
                    {
                        t_cpu_info *cpu = list_get(lista_cpus, i);
                        if (cpu->socket_dispatch == socket_cliente)
                        {
                            cpu->ocupada = 0;
                            cpu->pcb_ejecutando = NULL;
                            cpu->quantum_vigente = 0;
                            break;
                        }
                    }
                    pthread_mutex_unlock(&mx_lista_cpus);

                    sem_post(&sem_cpus_libres);

                    pcb_padre->estado = READY;
                    log_info(logger,
                             "## (%d) Pasa del estado EXEC al estado READY",
                             pcb_padre->pid);

                    pthread_mutex_lock(&mx_cola_ready);
                    list_add(colas_ready[pcb_padre->prioridad], pcb_padre);
                    pthread_mutex_unlock(&mx_cola_ready);

                    sem_post(&sem_procesos_ready);
                }

                break;
            }
            case S_MUTEX_CREATE:
            {
                // Consiste en crear un mutex...
                char *nombre = (char *)list_get(datos, 2);

                ejecutar_mutex_create(nombre);
                pthread_mutex_lock(&mx_lista_cpus); // libera cpu

                for (int i = 0; i < list_size(lista_cpus); i++)
                {
                    t_cpu_info *cpu = list_get(lista_cpus, i);

                    if (cpu->socket_dispatch == socket_cliente)
                    {
                        cpu->ocupada = 0;
                        cpu->pcb_ejecutando = NULL;
                        cpu->quantum_vigente = 0;
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_lista_cpus);

                sem_post(&sem_cpus_libres);

                pcb->estado = READY; // el proc vuelve a ready

                pthread_mutex_lock(&mx_cola_ready);
                list_add(colas_ready[pcb->prioridad], pcb);
                pthread_mutex_unlock(&mx_cola_ready);
                sem_post(&sem_procesos_ready);
                break;
            }

            case S_MUTEX_LOCK:
            {
                // El proceso quiere tomar un mutex, puede pasar que este libre u ocupado
                char *nombre = (char *)list_get(datos, 2);

                ejecutar_mutex_lock(pcb, nombre);
                pthread_mutex_lock(&mx_lista_cpus); // liberar cpu

                for (int i = 0; i < list_size(lista_cpus); i++)
                {
                    t_cpu_info *cpu = list_get(lista_cpus, i);

                    if (cpu->socket_dispatch == socket_cliente)
                    {
                        cpu->ocupada = 0;
                        cpu->pcb_ejecutando = NULL;
                        cpu->quantum_vigente = 0;
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_lista_cpus);

                sem_post(&sem_cpus_libres);
                if (pcb->estado != BLOCK) // vuelve a ready si no quedó bloqueado
                {
                    pcb->estado = READY;

                    pthread_mutex_lock(&mx_cola_ready);
                    list_add(colas_ready[pcb->prioridad], pcb);
                    pthread_mutex_unlock(&mx_cola_ready);

                    sem_post(&sem_procesos_ready);
                }
                break;
            }

            case S_MUTEX_UNLOCK:
            {
                // El proceso libera un mutex y no tiene sentido sacar al proceso de la cpu
                // ya que se libera en microsegundos
                char *nombre = (char *)list_get(datos, 2);

                // 1. Libera el mutex internamente (y despierta al primer bloqueado si es que existe)
                ejecutar_mutex_unlock(pcb, nombre);

                // 2. No tocamos las CPUs, no tocamos READY. El proceso sigue en EXEC en su CPU.
                // Le mandamos un OK a la CPU para que sepa que la syscall terminó y siga con la próxima instrucción.
                op_code resultado = OK;
                send(socket_cliente, &resultado, sizeof(op_code), 0);

                break;
            }

            default:
                log_warning(logger, "Syscall no manejada: %d", tipo_syscall);
                break;
            }

            list_destroy_and_destroy_elements(datos, free);
            break;
        }
        case MOTIVO_INTERRUPCION:
        {
            // Lo envia cpu cuando scheduler lo interrumpio ya sea x quantum o prioridad
            t_list *datos = recibir_paquete(socket_cliente);
            int pid = *(int *)list_get(datos, 0);
            list_destroy_and_destroy_elements(datos, free);

            t_pcb *pcb_interrumpido = buscar_pcb_en_ejecucion(socket_cliente);
            if (pcb_interrumpido == NULL)
                break;

            t_estado ant = pcb_interrumpido->estado;
            pcb_interrumpido->estado = READY;
            log_info(logger, "## (%d) Pasa del estado %s al estado READY",
                     pid, estado_a_string(ant));

            pthread_mutex_lock(&mx_lista_cpus);
            for (int i = 0; i < list_size(lista_cpus); i++)
            {
                t_cpu_info *cpu = list_get(lista_cpus, i);
                if (cpu->socket_dispatch == socket_cliente)
                {
                    cpu->ocupada = 0;
                    cpu->pcb_ejecutando = NULL;
                    cpu->quantum_vigente = 0;
                    break;
                }
            }
            pthread_mutex_unlock(&mx_lista_cpus);
            sem_post(&sem_cpus_libres);

            pthread_mutex_lock(&mx_cola_ready);
            list_add(colas_ready[pcb_interrumpido->prioridad], pcb_interrumpido);
            pthread_mutex_unlock(&mx_cola_ready);
            sem_post(&sem_procesos_ready);
            break;
        }

        case STDIN_RESPUESTA:
        {
            // Lo envia io stdin cuando el usuario termino de escribir
            t_list *datos_io = recibir_paquete(socket_cliente);
            int pid_liberar = *(int *)list_get(datos_io, 0);
            void *contenido = list_get(datos_io, 1); // datos que escribió el usuario

            // Buscar el PCB (mismo patron que STDOUT/SLEEP: primero BLOCK, después SUSP.BLOCK)
            t_pcb *pcb_stdin = NULL;
            bool estaba_suspendido = false;

            pthread_mutex_lock(&mx_cola_block);
            for (int i = 0; i < list_size(cola_block); i++)
            {
                t_pcb *p = list_get(cola_block, i);
                if (p->pid == pid_liberar)
                {
                    pcb_stdin = list_remove(cola_block, i);
                    break;
                }
            }
            pthread_mutex_unlock(&mx_cola_block);

            if (pcb_stdin == NULL)
            {
                pthread_mutex_lock(&mx_cola_susp_block);
                for (int i = 0; i < list_size(cola_susp_block); i++)
                {
                    t_pcb *p = list_get(cola_susp_block, i);
                    if (p->pid == pid_liberar)
                    {
                        pcb_stdin = list_remove(cola_susp_block, i);
                        estaba_suspendido = true;
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_cola_susp_block);
            }

            if (pcb_stdin == NULL)
            {
                log_warning(logger, "STDIN_RESPUESTA de un PID no encontrado en BLOCK ni SUSP.BLOCK: %d", pid_liberar);
                list_destroy_and_destroy_elements(datos_io, free);
                break;
            }

            list_destroy_and_destroy_elements(datos_io, free);

            // Escribir en memoria lo que tipeo el usuario, usando dir_logica y tamaño
            // que guardamos en el PCB durante S_STDIN
            t_paquete *p_escritura = crear_paquete(SOLICITUD_ESCRITURA);
            agregar_a_paquete(p_escritura, &pcb_stdin->pid, sizeof(uint32_t));
            agregar_a_paquete(p_escritura, &pcb_stdin->tamanio_io, sizeof(uint32_t));
            agregar_a_paquete(p_escritura, &pcb_stdin->dir_logica_io, sizeof(uint32_t));
            agregar_a_paquete(p_escritura, contenido, pcb_stdin->tamanio_io);
            enviar_paquete(p_escritura, socket_memoria);
            eliminar_paquete(p_escritura);

            // Esperar confirmación de KM
            int cod_resp = recibir_operacion(socket_memoria);
            if (cod_resp != RESPUESTA_ESCRITURA)
            {
                log_error(logger, "KM no confirmó la escritura de STDIN para el PID %d", pcb_stdin->pid);
                // Ver qué hacemos aca ¿lo mandamos a READY igual o lo dejamos bloqueado?
            }

            // Pasar a READY (igual al cierre de STDOUT/SLEEP)
            pcb_stdin->estado = READY;

            if (estaba_suspendido)
                log_info(logger, "## (%d) finalizó IO y pasa a READY / SUSP. READY", pcb_stdin->pid);
            else
                log_info(logger, "## (%d) finalizo IO y pasa a READY", pcb_stdin->pid);

            pthread_mutex_lock(&mx_cola_ready);
            list_add(colas_ready[pcb_stdin->prioridad], pcb_stdin);
            pthread_mutex_unlock(&mx_cola_ready);

            verificar_desalojo_por_prioridad(pcb_stdin);
            sem_post(&sem_procesos_ready);
            break;
        }
        case STDOUT_RESPUESTA:
        case SLEEP_RESPUESTA:
        {
            // Lo envia io sleep cuando finalizo el tiempo de espera
            t_list *datos_io = recibir_paquete(socket_cliente);
            int pid_liberar = *(int *)list_get(datos_io, 0);
            list_destroy_and_destroy_elements(datos_io, free);

            t_pcb *pcb_despierto = NULL;
            bool estaba_suspendido = false;

            // Intentamos buscarlo en la cola BLOCK normal
            pthread_mutex_lock(&mx_cola_block);
            for (int i = 0; i < list_size(cola_block); i++)
            {
                t_pcb *p = list_get(cola_block, i);
                if (p->pid == pid_liberar)
                {
                    pcb_despierto = list_remove(cola_block, i);
                    break;
                }
            }
            pthread_mutex_unlock(&mx_cola_block);

            // Si no estaba, significa que sufrio timeout y est en cola_susp_block
            if (pcb_despierto == NULL)
            {
                pthread_mutex_lock(&mx_cola_susp_block);
                for (int i = 0; i < list_size(cola_susp_block); i++)
                {
                    t_pcb *p = list_get(cola_susp_block, i);
                    if (p->pid == pid_liberar)
                    {
                        pcb_despierto = list_remove(cola_susp_block, i);
                        estaba_suspendido = true;
                        break;
                    }
                }
                pthread_mutex_unlock(&mx_cola_susp_block);
            }

            if (pcb_despierto != NULL)
            {
                pcb_despierto->estado = READY;

                if (estaba_suspendido)
                {
                    log_info(logger, "## (%d) finalizó IO y pasa a READY / SUSP. READY", pcb_despierto->pid);
                }

                else
                {
                    log_info(logger, "## (%d) finalizo IO y pasa a READY", pcb_despierto->pid);
                }

                pthread_mutex_lock(&mx_cola_ready);
                list_add(colas_ready[pcb_despierto->prioridad], pcb_despierto);
                pthread_mutex_unlock(&mx_cola_ready);

                verificar_desalojo_por_prioridad(pcb_despierto);
                sem_post(&sem_procesos_ready);
            }
            break;
        }

        default:
            log_warning(logger, "Operacion desconocida");
            break;
        } // Cierra el switch(cod_op)
    } // Cierra el while(1)
} // Cierra atender_cliente

t_pcb *crear_pcb(uint32_t pid, uint32_t prioridad)
{                                       // DEvuelve un puntero y los datos que pasamos de afuera, que tan importante es
    t_pcb *pcb = malloc(sizeof(t_pcb)); // Reserva espacio

    pcb->pid = pid;
    pcb->prioridad = prioridad;
    pcb->prioridad_original = prioridad;
    pcb->estado = NEW; // Todos nacen en NEW

    // Inicializamos el contexto (los registros en 0)
    pcb->contexto = malloc(sizeof(t_contexto));
    pcb->contexto->registros.PC = 0;
    pcb->contexto->registros.AX = 0;
    pcb->contexto->registros.BX = 0;
    pcb->contexto->registros.CX = 0;
    pcb->contexto->registros.DX = 0;
    pcb->contexto->registros.EAX = 0;
    pcb->contexto->registros.EBX = 0;
    pcb->contexto->registros.ECX = 0;
    pcb->contexto->registros.EDX = 0;
    pcb->contexto->registros.SI = 0;
    pcb->contexto->registros.DI = 0;

    return pcb;
}

void *planificador_largo_plazo(void *arg)
{

    while (1)
    {
        // esperando a que alguien haga sem_post(sem_procesos_new)
        sem_wait(&sem_procesos_new);

        // Saca el proceso de NEW con seguridad
        pthread_mutex_lock(&mx_cola_new);
        t_pcb *pcb = list_remove(cola_new, 0);
        pthread_mutex_unlock(&mx_cola_new);

        // se pasa a READY
        t_estado estado_anterior = pcb->estado;
        pcb->estado = READY;

        // se mete en READY
        pthread_mutex_lock(&mx_cola_ready);
        list_add(colas_ready[pcb->prioridad], pcb);

        log_info(logger, "## (%d) Pasa del estado %s al estado READY",
                 pcb->pid, estado_a_string(estado_anterior));
        pthread_mutex_unlock(&mx_cola_ready);

        // APUNTE: Verificamos si el proceso nuevo ingresado de NEW desaloja a alguien en CPU
        verificar_desalojo_por_prioridad(pcb);

        sem_post(&sem_procesos_ready);
    }
    return NULL;
}

void *planificador_corto_plazo(void *arg)
{
    while (1)
    {
        // APUNTE: Si se esta compactando, el hilo duerme un instante para no consumir CPU y frena el despacho
        if (compactacion_en_progreso)
        {
            usleep(10000);
            continue;
        }

        sem_wait(&sem_procesos_ready);

        // Esperamos que alguna CPU se conecte y este libre
        sem_wait(&sem_cpus_libres);

        // Sacamos el proceso de la cola READY con seguridad usando el mutex
        pthread_mutex_lock(&mx_cola_ready);
        t_pcb *pcb = NULL;

        for (int i = 0; i < MAX_PRIORIDADES; i++)
        {
            if (!list_is_empty(colas_ready[i]))
            {
                pcb = list_remove(colas_ready[i], 0);
                break; // Cortamos el bucle al encontrar el mas prioritario
            }
        }
        pthread_mutex_unlock(&mx_cola_ready);

        // Actualizamos el estado del proceso a EXEC (Ejecucion)
        t_estado estado_anterior = pcb->estado;
        pcb->estado = EXEC;
        log_info(logger, "## (%d) Pasa del estado %s al estado EXEC",
                 pcb->pid, estado_a_string(estado_anterior));

        // Buscamos en nuestra lista cual es la CPU que esta libre
        pthread_mutex_lock(&mx_lista_cpus);
        for (int i = 0; i < list_size(lista_cpus); i++)
        {
            t_cpu_info *cpu = list_get(lista_cpus, i);

            if (cpu->ocupada == 0)
            {
                // Marcamos que esta CPU ahora tiene un trabajo asignado
                cpu->ocupada = 1;
                cpu->pcb_ejecutando = pcb;

                // --- NUEVO PROTOCOLO ESTRUCTURADO ---

                // Preparamos el codigo de operacion que definimos en protocolo.h
                op_code cod_despacho = DESPACHAR_PROCESO;
                uint32_t pid_a_enviar = pcb->pid;

                // 2Enviamos el "QUE" vamos a hacer (el codigo de operacion)
                // Usamos sizeof(op_code) para que sea consistente con el enum
                send(cpu->socket_dispatch, &cod_despacho, sizeof(op_code), 0);

                // 3Enviamos el "DATO" (el numero de PID)
                send(cpu->socket_dispatch, &pid_a_enviar, sizeof(uint32_t), 0);

                log_info(logger, "## (%d) Enviado a CPU %d usando operacion DESPACHAR_PROCESO",
                         pcb->pid, cpu->cpu_id);

                // ============================================================
                // >>> NUEVA LOGICA PARA ROUND ROBBIN <<<
                // ============================================================
                if (algoritmo_actual == RR)
                {
                    // Creamos la estructura con los datos necesarios para el hilo del reloj
                    t_datos_quantum *args_quantum = malloc(sizeof(t_datos_quantum));
                    args_quantum->socket_cpu = cpu->socket_interrupt; // Ruteo por el puerto 8003
                    args_quantum->pid = pcb->pid;
                    args_quantum->quantum_vigente = &cpu->quantum_vigente; // ← pasás la dirección
                    // Lanzamos el hilo del quantum en paralelo para que cuente el tiempo
                    pthread_t t_quantum;
                    pthread_create(&t_quantum, NULL, hilo_quantum, args_quantum);
                    cpu->hilo_quantum_activo = t_quantum;
                    cpu->quantum_vigente = 1;
                    pthread_detach(t_quantum); // Lo separamos para que se limpie solo al terminar
                } // ====================================================

                break; // Ya mandamos el proceso, salimos del for
            }
        }
        pthread_mutex_unlock(&mx_lista_cpus);
    }
    return NULL;
}

void *hilo_quantum(void *arg)
{
    t_datos_quantum *datos = (t_datos_quantum *)arg;

    // Duermo el tiempo del quantum (en milisegundos)
    // usleep trabaja en microsegundos, por eso multiplico por 1000
    usleep(quantum_ms * 1000);
    if (*(datos->quantum_vigente))
    {
        op_code tipo = FIN_QUANTUM;
        // APUNTE: Mandamos la interrupcion por el socket global del puerto 8003
        send(datos->socket_cpu, &tipo, sizeof(op_code), 0);
    }
    // Cuando se acaba el tiempo, mando señal de interrupción a la CPU
    // El tipo de mensaje tiene que estar definido en nuestro protocolo

    /*
    //esto ya queda viejo con la nueva implementacion
    int tipo_interrupcion = 8; // FIN_QUANTUM SERIA LA CONSTANTE A DEFINIR, puse 8 para compilar
    send(datos->socket_cpu, &tipo_interrupcion, sizeof(int), 0);
*/
    log_info(logger, "## (%d) - Desalojado por fin de quantum", datos->pid);

    free(datos);
    return NULL;
}
// Buscamos el mutex (si es que existe)
t_mutex_kernel *buscar_mutex(char *nombre)
{
    pthread_mutex_lock(&mx_lista_mutex);

    for (int i = 0; i < list_size(lista_mutex_globales); i++)
    {
        t_mutex_kernel *m = list_get(lista_mutex_globales, i);
        if (strcmp(m->nombre, nombre) == 0)
        {
            pthread_mutex_unlock(&mx_lista_mutex);
            return m;
        }
    }

    pthread_mutex_unlock(&mx_lista_mutex);
    return NULL; // Si llegamos aca el MUTEX no existe
}

// Creacion de un MUTEX
void ejecutar_mutex_create(char *nombre_mutex)
{
    t_mutex_kernel *nuevo_mutex = malloc(sizeof(t_mutex_kernel));
    nuevo_mutex->nombre = strdup(nombre_mutex);
    nuevo_mutex->pid_dueno = -1; // Empieza libre
    nuevo_mutex->prioridad_original_dueno = 0;
    nuevo_mutex->cola_bloqueados = list_create();
    pthread_mutex_init(&(nuevo_mutex->mx_mutex), NULL);

    pthread_mutex_lock(&mx_lista_mutex);
    list_add(lista_mutex_globales, nuevo_mutex);
    pthread_mutex_unlock(&mx_lista_mutex);

    log_info(logger, "MUTEX creado: %s", nombre_mutex);
}

void ejecutar_mutex_lock(t_pcb *pcb_solicitante, char *nombre_mutex)
{
    t_mutex_kernel *m = buscar_mutex(nombre_mutex);

    if (m == NULL)
    {
        log_error(logger, "Error: El proceso %d quiso usar un mutex inexistente: %s", pcb_solicitante->pid, nombre_mutex);
        // POdria finalizar el proceso aca, por ahora solo logueamos
        return;
    }

    pthread_mutex_lock(&(m->mx_mutex));

    if (m->pid_dueno == -1)
    {
        // CASO 1: El mutex esta libre
        m->pid_dueno = pcb_solicitante->pid;
        m->prioridad_original_dueno = pcb_solicitante->prioridad;
        log_info(logger, "## (%d) Toma el Mutex %s", pcb_solicitante->pid, nombre_mutex);
        pthread_mutex_unlock(&(m->mx_mutex));

        // Como el proceso ya tiene el mutex, sigue ejecutando (se lo mandas de vuelta a la CPU)
        // Esto depende de como manejes el ciclo de instruccion, por ahora no lo bloqueamos
        /*
        //SAQUE ESTAS LINEAS XQ ANTENDER_CLIENTE YA HACE EXACTAMENTE ESTO
        pcb_solicitante->estado = READY; // LIsto para volver a ejecutar
        pthread_mutex_lock(&mx_cola_ready); // Cerramos la cola READY PAra que ningun hilo la rompa
        list_add(cola_ready, pcb_solicitante); // Metemos fisicamente el proceso al final de la lista de espera (cola de READY)
        pthread_mutex_unlock(&mx_cola_ready); // Abrimos el candado para que otros hilos del Kernel puedan volver a usar la lista de READY
        sem_post(&sem_procesos_ready); // Incrementamos el semaforo contador.
*/
    }
    else
    {
        log_info(logger, "## (%d) Pasa del estado EXEC al estado BLOCK", pcb_solicitante->pid);

        pcb_solicitante->estado = BLOCK;
        list_add(m->cola_bloqueados, pcb_solicitante);

        t_pcb *pcb_dueno = buscar_pcb_por_pid(m->pid_dueno);
        if (pcb_dueno != NULL)
        {
            if (pcb_solicitante->prioridad < pcb_dueno->prioridad)
            {
                uint32_t prioridad_anterior = pcb_dueno->prioridad;
                pcb_dueno->prioridad = pcb_solicitante->prioridad;

                log_info(logger, "## %d Cambio de prioridad: %d - %d", pcb_dueno->pid, prioridad_anterior, pcb_dueno->prioridad);
            }

            // Si el dueño estaba en READY, lo reinsertamos en la cola del nuevo nivel heredado
            if (pcb_dueno->estado != EXEC)
            {
                pthread_mutex_lock(&mx_cola_ready);
                list_add(colas_ready[pcb_dueno->prioridad], pcb_dueno);
                pthread_mutex_unlock(&mx_cola_ready);
            }
        }

        pthread_mutex_unlock(&(m->mx_mutex));
        sem_post(&sem_cpus_libres);
    }
}

void ejecutar_mutex_unlock(t_pcb *pcb_dueno, char *nombre_mutex)
{
    t_mutex_kernel *m = buscar_mutex(nombre_mutex);

    if (m == NULL || m->pid_dueno != pcb_dueno->pid)
    {
        return; // El proceso no es el dueno o no existe el mutex
    }

    pthread_mutex_lock(&(m->mx_mutex));

    log_info(logger, "## (%d) Libera el Mutex %s", pcb_dueno->pid, nombre_mutex);

    if (list_is_empty(m->cola_bloqueados))
    {
        // No hay nadie esperando
        m->pid_dueno = -1;
        pthread_mutex_unlock(&(m->mx_mutex));
    }

    else
    {
        t_pcb *pcb_desbloqueado = list_remove(m->cola_bloqueados, 0);

        // APUNTE: Antes de soltar el mutex, el dueno actual recupera su fuerza real si habia heredado
        if (pcb_dueno->prioridad != m->prioridad_original_dueno)
        {
            uint32_t prioridad_anterior = pcb_dueno->prioridad;
            pcb_dueno->prioridad = m->prioridad_original_dueno;
            log_info(logger, "## %d Cambio de prioridad: %d - %d", pcb_dueno->pid, prioridad_anterior, pcb_dueno->prioridad);
        }

        m->pid_dueno = pcb_desbloqueado->pid;
        m->prioridad_original_dueno = pcb_desbloqueado->prioridad;

        pthread_mutex_unlock(&(m->mx_mutex));

        pcb_desbloqueado->estado = READY;
        pthread_mutex_lock(&mx_cola_ready);
        list_add(colas_ready[pcb_desbloqueado->prioridad], pcb_desbloqueado);
        pthread_mutex_unlock(&mx_cola_ready);

        log_info(logger, "## (%d) Pasa de BLOCK a READY (obtuvo mutex %s)", pcb_desbloqueado->pid, nombre_mutex);

        // APUNTE: Verificamos si el proceso que gano el mutex desaloja la CPU actual por prioridad
        verificar_desalojo_por_prioridad(pcb_desbloqueado);

        sem_post(&sem_procesos_ready);
    }
}

t_pcb *buscar_pcb_en_ejecucion(int socket_fd)
{
    t_pcb *pcb_encontrado = NULL;
    pthread_mutex_lock(&mx_lista_cpus);

    for (int i = 0; i < list_size(lista_cpus); i++)
    {
        t_cpu_info *cpu = list_get(lista_cpus, i);
        if (cpu->socket_dispatch == socket_fd)
        {
            pcb_encontrado = cpu->pcb_ejecutando;
            break;
        }
    }

    pthread_mutex_unlock(&mx_lista_cpus);
    return pcb_encontrado;
}

t_pcb *buscar_pcb_por_pid(uint32_t pid)
{
    // Buscamos en la lista de CPUs a ver si esta ejecutando
    pthread_mutex_lock(&mx_lista_cpus);
    for (int i = 0; i < list_size(lista_cpus); i++)
    {
        t_cpu_info *cpu = list_get(lista_cpus, i);
        if (cpu->pcb_ejecutando != NULL && cpu->pcb_ejecutando->pid == pid)
        {
            t_pcb *encontrado = cpu->pcb_ejecutando;
            pthread_mutex_unlock(&mx_lista_cpus);
            return encontrado;
        }
    }
    pthread_mutex_unlock(&mx_lista_cpus);

    // Si no esta en CPU (Buscado con el anterior if), puede estar en alguna de las colas de READY
    pthread_mutex_lock(&mx_cola_ready);
    for (int i = 0; i < MAX_PRIORIDADES; i++)
    {
        for (int j = 0; j < list_size(colas_ready[i]); j++)
        {
            t_pcb *p = list_get(colas_ready[i], j);
            if (p->pid == pid)
            {
                // Lo removemos temporalmente para actualizar su prioridad de forma segura
                t_pcb *encontrado = list_remove(colas_ready[i], j);
                pthread_mutex_unlock(&mx_cola_ready);
                return encontrado;
            }
        }
    }
    pthread_mutex_unlock(&mx_cola_ready);

    return NULL;
}

void liberar_mutex_de_proceso_saliente(t_pcb *pcb_saliente)
{
    pthread_mutex_lock(&mx_lista_mutex);

    for (int i = 0; i < list_size(lista_mutex_globales); i++)
    {
        t_mutex_kernel *m = list_get(lista_mutex_globales, i);

        // Si el proceso que termina es el dueño del mutex
        if (m->pid_dueno == pcb_saliente->pid)
        {
            log_info(logger, "## (%d) Finalizo - Liberando Mutex atrapado: %s", pcb_saliente->pid, m->nombre);

            // Liberamos el mutex
            pthread_mutex_lock(&(m->mx_mutex));

            if (list_is_empty(m->cola_bloqueados))
            {
                m->pid_dueno = -1;
            }
            else
            {
                t_pcb *proximo = list_remove(m->cola_bloqueados, 0);
                m->pid_dueno = proximo->pid;

                // Lo mandamos a READY
                proximo->estado = READY;
                pthread_mutex_lock(&mx_cola_ready);
                list_add(colas_ready[proximo->prioridad], proximo);
                pthread_mutex_unlock(&mx_cola_ready);

                sem_post(&sem_procesos_ready);
            }

            pthread_mutex_unlock(&(m->mx_mutex));
        }
    }

    pthread_mutex_unlock(&mx_lista_mutex);
}

void *escuchar_puerto_interrupt(void *args)
{
    int server_interrupt = iniciar_servidor("8003");

    if (server_interrupt == -1)
    {
        log_error(logger, "No se pudo levantar servidor en puerto 8003");
        return NULL;
    }

    log_info(logger, "Servidor de Interrupciones listo en puerto 8003");

    // Aceptamos múltiples CPUs
    while (1)
    {
        int socket_cliente = esperar_cliente(server_interrupt);
        if (socket_cliente >= 0)
        {
            socket_cpu_interrupt = socket_cliente;
            log_info(logger, "CPU conectada al puerto de interrupciones");
        }
    }
    return NULL;
}

void verificar_desalojo_por_prioridad(t_pcb *proceso_entrante)
{
    if (algoritmo_actual != CMN || !preemption_habilitada)
    {
        return;
    }

    pthread_mutex_lock(&mx_lista_cpus);
    for (int i = 0; i < list_size(lista_cpus); i++)
    {
        t_cpu_info *cpu = list_get(lista_cpus, i);

        // Si la CPU esta ocupada y el proceso ejecutando tiene MENOR prioridad (el numero mas alto)
        if (cpu->ocupada == 1 && cpu->pcb_ejecutando != NULL && cpu->pcb_ejecutando->prioridad > proceso_entrante->prioridad)
        {

            log_info(logger, "## (%d) Prioridad: %d - Desalojado por cola mas prioritaria por el proceso %d con prioridad %d",
                     cpu->pcb_ejecutando->pid, cpu->pcb_ejecutando->prioridad, proceso_entrante->pid, proceso_entrante->prioridad);

            cpu->quantum_vigente = 0;


            // MANDAMOS LA INTERRUPCION AL SOCKET ESPECIFICO DE ESTA CPU
            op_code tipo_int = FIN_QUANTUM;
            send(cpu->socket_interrupt, &tipo_int, sizeof(op_code), 0);
            int pid = cpu->pcb_ejecutando->pid; //probamos esto para no mandar opcode, CPU espera pid
            send(cpu->socket_interrupt, &pid, sizeof(int), 0);

            break;
        }
    }
    pthread_mutex_unlock(&mx_lista_cpus);
}

void *hilo_temporizador_suspension(void *arg)
{
    t_suspension_args *args = (t_suspension_args *)arg;

    // Esperamos el tiempo configurado antes de suspender
    usleep(args->tiempo_bloqueado_ms * 1000);

    pthread_mutex_lock(&mx_cola_block);
    t_pcb *pcb_a_suspender = NULL;

    // Buscamos si el proceso todavia sigue bloqueado en la cola BLOCK normal
    for (int i = 0; i < list_size(cola_block); i++)
    {
        t_pcb *p = list_get(cola_block, i);
        if (p->pid == args->pid)
        {
            pcb_a_suspender = list_remove(cola_block, i);
            break;
        }
    }
    pthread_mutex_unlock(&mx_cola_block);

    // Si no es NULL, significa que la IO tardO¿ y corresponde suspenderlo
    if (pcb_a_suspender != NULL)
    {
        t_estado ant = pcb_a_suspender->estado;
        pcb_a_suspender->estado = BLOCK; // Mantiene el estado interno como BLOCK según el modelo de 7 estados

        log_info(logger, "## (%d) Pasa del estado %s al estado SUSP_BLOCK [TIMEOUT]",
                 pcb_a_suspender->pid, estado_a_string(ant));

        pthread_mutex_lock(&mx_cola_susp_block);
        list_add(cola_susp_block, pcb_a_suspender);
        pthread_mutex_unlock(&mx_cola_susp_block);

        // --- CORRECCION PROTOCOLO: Enviar orden de SUSPENDER a Kernel Memory ---
        t_paquete *paquete = crear_paquete(SUSPENDER_PROCESO);
        agregar_a_paquete(paquete, &(pcb_a_suspender->pid), sizeof(uint32_t));

        // ENVIAMOS Y EN PROCESO LIBERAMOS LA MEMORIA DEL PAQUETE
        enviar_paquete(paquete, socket_memoria);
        eliminar_paquete(paquete);
    }

    free(args);
    return NULL;
}

void *escuchar_kernel_memory(void *arg)
{
    while (1)
    {
        int cod_op = recibir_operacion(socket_memoria);

        if (cod_op == -1)
        {
            log_error(logger, "Se perdio conexion con Kernel Memory. Abortando Kernel Scheduler por seguridad.");
            log_destroy(logger);
            exit(EXIT_FAILURE); // Saca al Scheduler
        }

        switch (cod_op)
        {
        /*case MENSAJE:
        {
            recibir_mensaje(socket_memoria);
            break;
        }*/
        case SEGMENTO_CREADO_CORRECTAMENTE:
        {
            t_list *datos = recibir_paquete(socket_memoria);
            int pid_desbloquear = *(int *)list_get(datos, 0);
            list_destroy_and_destroy_elements(datos, free);

            t_pcb *pcb = NULL;
            pthread_mutex_lock(&mx_cola_block);
            for (int i = 0; i < list_size(cola_block); i++)
            {
                t_pcb *p = list_get(cola_block, i);
                if (p->pid == pid_desbloquear)
                {
                    pcb = list_remove(cola_block, i);
                    break;
                }
            }
            pthread_mutex_unlock(&mx_cola_block);

            if (pcb != NULL)
            {
                pcb->estado = READY;
                log_info(logger, "## (%d) Segmento creado, pasa a READY", pid_desbloquear);
                pthread_mutex_lock(&mx_cola_ready);
                list_add(colas_ready[pcb->prioridad], pcb);
                pthread_mutex_unlock(&mx_cola_ready);
                sem_post(&sem_procesos_ready);
            }
            break;
        }

        case AVISO_COMPACTACION:
        {
            log_info(logger, "## Inicio de compactacion");
            compactacion_en_progreso = true;

            pthread_mutex_lock(&mx_lista_cpus);
            for (int i = 0; i < list_size(lista_cpus); i++)
            {
                t_cpu_info *cpu = list_get(lista_cpus, i);
                if (cpu->ocupada == 1 && cpu->pcb_ejecutando != NULL)
                {
                    t_pcb *pcb_desalojado = cpu->pcb_ejecutando;
                    pcb_desalojado->estado = READY;
                    cpu->quantum_vigente = 0;

                    op_code msg = FIN_QUANTUM;
                    send(socket_cpu_interrupt, &msg, sizeof(op_code), 0);

                    pthread_mutex_lock(&mx_cola_ready);
                    list_add_in_index(colas_ready[pcb_desalojado->prioridad], 0, pcb_desalojado);
                    pthread_mutex_unlock(&mx_cola_ready);

                    cpu->ocupada = 0;
                    cpu->pcb_ejecutando = NULL;

                    sem_post(&sem_cpus_libres);
                    sem_post(&sem_procesos_ready);
                }
            }
            pthread_mutex_unlock(&mx_lista_cpus);
            int ok = OK;
            send(socket_memoria, &ok, sizeof(ok), 0);

            break;
        }

        case COMPACTACION_RECIBIDA:
        {
            log_info(logger, "## Fin de compactacion");
            compactacion_en_progreso = false;

            intentar_desuspender_procesos();
            break;
        }
        case OK:
        {

            // KM confirma que procesó algo correctamente (carga proceso, finalizar, etc)
            // Solo logueamos, no necesitamos hacer nada más
            log_info(logger, "KM respondio OK");
            ultima_respuesta_km = OK;
            sem_post(&sem_respuesta_memoria); // probando p/problema hilos init_proc
            break;
        }

        case ERROR:
        {

            log_warning(logger, "KM respondio ERROR");
            ultima_respuesta_km = ERROR;
            sem_post(&sem_respuesta_memoria); // Le avisamos al semaforo tmb para que destrabe el wait y evalue el error
            break;
        }

        case ERROR_CREACION_SEGMENTO:
        {
            t_list *datos = recibir_paquete(socket_memoria);
            int pid = *(int *)list_get(datos, 0);
            list_destroy_and_destroy_elements(datos, free);

            // El segmento no se pudo crear, finalizar el proceso con SEG_FAULT
            log_error(logger, "## (%d) finalizo su ejecucion con motivo de SEG_FAULT", pid);

            // Buscar el PCB en cola_block y moverlo a EXIT
            pthread_mutex_lock(&mx_cola_block);
            t_pcb *pcb = NULL;
            for (int i = 0; i < list_size(cola_block); i++)
            {
                t_pcb *p = list_get(cola_block, i);
                if (p->pid == pid)
                {
                    pcb = list_remove(cola_block, i);
                    break;
                }
            }
            pthread_mutex_unlock(&mx_cola_block);

            if (pcb != NULL)
            {
                liberar_mutex_de_proceso_saliente(pcb);
                enviar_finalizacion_proceso(socket_memoria, pid);
                /// sem_wait(&respuesta_memoria);
                pcb->estado = EXIT;
                list_add(cola_exit, pcb);
            }
            break;
        }

        case NUEVO_MEMORY_STICK:
        {
            // KM avisa que hay más memoria disponible
            log_info(logger, "Nuevo Memory Stick conectado, mas memoria disponible");
            // Acá podría des-suspender procesos, por ahora solo logueamos

            intentar_desuspender_procesos();
            break;
        }

        case MEMORIA_CORRUPTA:
        {
            // Un Memory Stick se desconectó, BSOD
            log_error(logger, "## MEMORIA CORRUPTA - Blue Screen of Death");
            // Finalizar todos los procesos y terminar
            exit(EXIT_FAILURE);
            break;
        }
        default:
            log_warning(logger, "Mensaje desconocido de KM: %d", cod_op);
            break;
        }
    }
    return NULL;
}

void intentar_desuspender_procesos()
{
    pthread_mutex_lock(&mx_cola_susp_block);

    // Si no hay nadie suspendido, no hacemos nada
    if (list_is_empty(cola_susp_block))
    {
        pthread_mutex_unlock(&mx_cola_susp_block);
        return;
    }

    // Buscamos(el primero con el nUmero de prioridad mAs bajo)
    int index_candidato = -1;
    t_pcb *candidato = NULL;

    for (int i = 0; i < list_size(cola_susp_block); i++)
    {
        t_pcb *p = list_get(cola_susp_block, i);
        if (candidato == NULL || p->prioridad < candidato->prioridad)
        {
            candidato = p;
            index_candidato = i;
        }
    }

    if (candidato != NULL)
    {
        log_info(logger, "## Evaluando desuspension del PID %d", candidato->pid);

        // Le preguntamos a Kernel Memory si hay espacio
        t_paquete *paquete = crear_paquete(DESUSPENDER_PROCESO);
        agregar_a_paquete(paquete, &(candidato->pid), sizeof(uint32_t));
        enviar_paquete(paquete, socket_memoria);
        eliminar_paquete(paquete);

        // Esperamos la respuesta de Kernel Memory
        int respuesta = recibir_operacion(socket_memoria);

        if (respuesta == OK) // KM dice que hay espacio en los Memory Sticks y ya restauro los segmentos desde SWAP
        {
            // Lo sacamosde la cola de suspendidos
            list_remove(cola_susp_block, index_candidato);

            // Cambiamos su estado y vuelve a la cola READY normal
            t_estado ant = candidato->estado;
            candidato->estado = READY;

            // Log
            log_info(logger, "## (%d) finalizO IO y pasa a READY / SUSP. READY", candidato->pid);

            pthread_mutex_lock(&mx_cola_ready);
            list_add(colas_ready[candidato->prioridad], candidato);
            pthread_mutex_unlock(&mx_cola_ready);

            // Verificamos si este proceso desbanca a alguien en CPU
            verificar_desalojo_por_prioridad(candidato);

            sem_post(&sem_procesos_ready);
        }
        else
        {
            log_info(logger, "## Kernel Memory indico que NO hay espacio contiguo todavia para el PID %d", candidato->pid);
        }
    }

    pthread_mutex_unlock(&mx_cola_susp_block);
}