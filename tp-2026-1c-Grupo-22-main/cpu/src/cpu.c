
#include <stdio.h>
#include <string.h>
// #include "registros.h" pusimos todo en utils/protocolo.h
#include <utils/utils.h>
#include <utils/protocolo.h>

#include "ciclo.h"
#include <stdbool.h> //para mutex hilo?
#include <pthread.h>
t_log_level log_level;
t_log *logger;
t_config *config;
uint32_t tam_max_segmento; // = 256; // recibir de KMemory cuando lo implementen

//------ para cpu.h?
/*typedef struct
{
    uint32_t id;
    uint32_t base; // lo último que me envía KM, base y limite
    uint32_t limite;
    int socket;
    char ip[50];
    char puerto[20];
} t_memory_stick_cpu;*/ //lo pusimos en ciclo.h

extern t_list* lista_memory_sticks;
extern pthread_mutex_t mx_memory_sticks;

//void agregar_memory_stick(int id, char* ip, char* puerto,int socket); //cambio ahora 19/6
void agregar_memory_stick(int id, uint32_t base, uint32_t limite, char* ip, char* puerto, int socket);
//------------para MS
typedef struct
{
    uint32_t id;
    uint32_t base;
    uint32_t limite;
    char ip[50];
    char puerto[20];
} t_info_memory_stick;
t_list* lista_memory_sticks;
pthread_mutex_t mx_memory_sticks = PTHREAD_MUTEX_INITIALIZER;

// Kernel Memory
char *ip_kernel_memory;
char *puerto_kernel_memory;
int socket_kernel_memory;

// Kernel Scheduler
char *ip_kernel_scheduler;
char *puerto_kernel_scheduler;
int socket_kernel_scheduler;
char *puerto_interrupt;
int socket_scheduler_interrupt;

// para interrupciones
// bool hay_interrupcion = false;   // ← falta esta
volatile bool hay_interrupcion = false; /*le dice al compilador "esta variable puede
cambiar en cualquier momento desde afuera, siempre leela de memoria, no la caches".*/
int pid_interrupcion = -1;
pthread_mutex_t mutex_interrupcion = PTHREAD_MUTEX_INITIALIZER;
// Memory Stick
char *ip_memory_stick;
char *puerto_memory_stick;
int socket_memory_stick;

void ObtenerConfig(void);
void *escuchar_interrupciones(void *arg);
// void *simular_interrupcion(void *arg); // hardcodeado de prueba
t_contexto *pedir_contexto(int socket, int pid);
// int conectar_con_reintento(char* ip, char* puerto);

int main(int argc, char *argv[])
{
    // ✔️ Validación de argumento
    if (argc < 2)
    {
        printf("Uso: %s [ID_CPU]\n", argv[0]);
        return 1;
    }

    // ✔️ Leer ID
    int id_cpu = atoi(argv[1]);

    saludar("cpu");
    ObtenerConfig();
    //lista para los próximos MS
    lista_memory_sticks = list_create();
    // ✔️ Logger por CPU
    char log_name[50];
    sprintf(log_name, "cpu_%d.log", id_cpu);

    char process_name[20];
    sprintf(process_name, "CPU %d", id_cpu);
    // creamos un logger por cada ID del CPU
    logger = log_create(log_name, process_name, true, log_level);

    printf("IP KERNEL MEMORY: %s\n", ip_kernel_memory);
    printf("PUERTO KERNEL MEMORY: %s\n", puerto_kernel_memory);
    //printf("IP MEMORY STICK: %s\n", ip_memory_stick);
    //printf("PUERTO MEMORY STICK: %s\n", puerto_memory_stick);
    printf("IP KERNEL MEMORY: %s\n", ip_kernel_scheduler);
    printf("PUERTO KERNEL SCHEDULER: %s\n", puerto_kernel_scheduler);

    // logger = log_create("cpu.log", "CPU", true, log_level); ya creamos logger antes?
    //  🔌 Conexiones

    socket_kernel_memory = conectar(ip_kernel_memory, puerto_kernel_memory);
    //socket_memory_stick = conectar(ip_memory_stick, puerto_memory_stick);//ahora recibe la info de KM
    socket_kernel_scheduler = conectar(ip_kernel_scheduler, puerto_kernel_scheduler);
    socket_scheduler_interrupt = conectar(ip_kernel_scheduler, puerto_interrupt);

    // por fin hilo que escucha interrupciones en paralelo
    pthread_t hilo_interrupt;
    pthread_create(&hilo_interrupt, NULL, escuchar_interrupciones, NULL);
    // solo  para probar interrupción hardcodeada
    /*pthread_t hilo_sim;
    pthread_create(&hilo_sim, NULL, simular_interrupcion, NULL);
    pthread_detach(hilo_sim);*/

    printf("Socket kernel_memory: %d\n", socket_kernel_memory);
    printf("Socket memory_stick: %d\n", socket_memory_stick);

    // 📡 Mensajes de prueba
    if (socket_kernel_memory > 0)
    {
        int tipo = CPU;
        send(socket_kernel_memory, &tipo, sizeof(int), 0);
        send(socket_kernel_memory, &id_cpu, sizeof(int), 0);
        // recibir SEGMENT_MAX_SIZE de KMemory
        recv(socket_kernel_memory, &tam_max_segmento, sizeof(uint32_t), MSG_WAITALL);
        int cantidad_sticks;// otrorecv  de KM para administrar MS
        recv(socket_kernel_memory, &cantidad_sticks, sizeof(int), MSG_WAITALL);

        log_info(logger, "Kernel Memory informó %d Memory Sticks", cantidad_sticks);

        for(int i = 0; i < cantidad_sticks; i++)
{
    t_info_memory_stick info;
    recv(socket_kernel_memory, &info, sizeof(t_info_memory_stick), MSG_WAITALL);

    log_info(logger, "MS recibido: id=%d ip=%s puerto=%s", info.id, info.ip, info.puerto);

    int socket_ms = conectar(info.ip, info.puerto);
        if(socket_ms > 0)
    {
        //int tipo = CPU; // está 2 vecesen el  if de KM
        send(socket_ms, &tipo, sizeof(int), 0);
        send(socket_ms, &id_cpu, sizeof(int), 0);
        log_info(logger, "Conectado a MS %d", info.id);
    }
    //agregar_memory_stick(info.id, info.ip, info.puerto, socket_ms);
    agregar_memory_stick(info.id, info.base, info.limite, info.ip, info.puerto, socket_ms);
} // hasta acá para MSticks iniciales
        log_info(logger, "Cantidad almacenada: %d", list_size(lista_memory_sticks));
        log_info(logger, "Conectado a Kernel Memory como CPU"); // obligatorio
        log_info(logger, "Conectado a KMemory. SEGMENT_MAX_SIZE=%d", tam_max_segmento);
    }
    else
    {
        log_error(logger, "Error en conexión a kernel_memory");
    }

    /*if (socket_memory_stick > 0) //ahora dentro del if de KM?
    {
        int tipo = CPU;
        send(socket_memory_stick, &tipo, sizeof(int), 0);
        // enviar_mensaje("Conexion OK con memory_stick", socket_memory_stick);
        log_info(logger, "Conectado a Memory Stick como CPU");
    }
    else
    {
        log_error(logger, "Error en conexión a memory_stick");
    }*/
    if (socket_kernel_scheduler > 0) // con los nuevos CODE_OP??
    {
        // 1. mandar tipo
        int tipo = CPU;
        send(socket_kernel_scheduler, &tipo, sizeof(int), 0);

        // 2. esperar que el Scheduler pida el ID
        int cod_op;
        recv(socket_kernel_scheduler, &cod_op, sizeof(int), MSG_WAITALL);

        if (cod_op == PEDIR_ID_CPU)
        {
            // 3. responder con el ID
            int respuesta = RESPONDER_ID_CPU;
            send(socket_kernel_scheduler, &respuesta, sizeof(int), 0);
            send(socket_kernel_scheduler, &id_cpu, sizeof(int), 0);
            log_info(logger, "Handshake completado con Scheduler. ID=%d", id_cpu);
        }
    }


   if (socket_kernel_scheduler < 0)
{
    log_warning(logger, "Scheduler no conectado. CPU en modo prueba.");

    while (1)
    {
        sleep(1);
    }
}
    // bucle principal de la cpu
    while (1)
    {
        int cod_op;
        // 1. recibimos el codigo de operacion (despachar_proceso)
        // usamos msg_waitall para asegurar que el dato llegue completo
        int bytes = recv(socket_kernel_scheduler, &cod_op, sizeof(int), MSG_WAITALL);

        if (bytes <= 0)
        {
            log_error(logger, "se perdio la conexion con el scheduler");
            break;
        }

        if (cod_op == DESPACHAR_PROCESO)
        {
            int pid_actual;
            // 2. recibimos el pid que el scheduler mando para ejecutar
            recv(socket_kernel_scheduler, &pid_actual, sizeof(int), MSG_WAITALL);

            log_info(logger, "## pid: %d - iniciando ejecucion de turno", pid_actual);

            // 3. pedimos el contexto a la memoria usando el pid real
             log_info(logger, "## pid: %d - pidiendo contexto a KM", pid_actual);
            t_contexto *ctx = pedir_contexto(socket_kernel_memory, pid_actual);

            if (ctx == NULL)
            {
                log_error(logger, "No se pudo obtener el contexto del pid %d", pid_actual);
                continue;
            }

            // 4. ejecutamos el proceso (ciclo de instruccion)
            // esto devuelve el motivo por el cual la cpu frena (exit, syscall, etc)
            t_instruccion *ultima_syscall = NULL;
            op_code motivo = ejecutar_proceso(pid_actual, ctx, socket_kernel_memory, &ultima_syscall);
            
            // 5. devolvemos el contexto actualizado a la memoria antes de avisar al scheduler
            actualizar_contexto(socket_kernel_memory, pid_actual, ctx);

            // 6. preparamos el paquete para avisar al scheduler el resultado
            t_paquete *p = crear_paquete(motivo);
            agregar_a_paquete(p, &pid_actual, sizeof(int));

            // si el motivo fue una syscall, mandamos sus parametros
            if (motivo == MOTIVO_SYSCALL && ultima_syscall != NULL)
            {
                int tipo_syscall = ultima_syscall->tipo;
                agregar_a_paquete(p, &tipo_syscall, sizeof(int));

                switch (tipo_syscall)
                {
                case S_SLEEP:
                {
                    // sleep espera un argumento (tiempo)
                    int tiempo = atoi(ultima_syscall->args[0]);
                    agregar_a_paquete(p, &tiempo, sizeof(int));
                    break;
                }
                case S_MUTEX_CREATE:
                case S_MUTEX_LOCK:
                case S_MUTEX_UNLOCK:
                {
                    // estas syscalls esperan el nombre del recurso
                    char *nombre = ultima_syscall->args[0];
                    agregar_a_paquete(p, nombre, strlen(nombre) + 1);
                    break;
                }
                case S_MEM_ALLOC:
                {
                    // recibe id de segmento y tamaño
                    int id_seg = atoi(ultima_syscall->args[0]);
                    int tam = atoi(ultima_syscall->args[1]);
                    agregar_a_paquete(p, &id_seg, sizeof(int));
                    agregar_a_paquete(p, &tam, sizeof(int));
                    break;
                }
                case S_MEM_FREE:
                {
                    // recibe solo el id de segmento
                    int id_seg = atoi(ultima_syscall->args[0]);
                    agregar_a_paquete(p, &id_seg, sizeof(int));
                    break;
                }
                case S_STDOUT:
                case S_STDIN:
                {
                    // obtenemos los valores de los registros pasados por argumento
                    uint32_t dir = leer_registro(ctx, ultima_syscall->args[0]);
                    uint32_t tam = leer_registro(ctx, ultima_syscall->args[1]);
                    log_debug(logger, "Tamaño del registro %u", tam);
                    agregar_a_paquete(p, &dir, sizeof(uint32_t));
                    agregar_a_paquete(p, &tam, sizeof(uint32_t));
                    break;
                }
                case S_INIT_PROC:
                {
                    // recibe path del archivo y prioridad inicial
                    char *archivo = ultima_syscall->args[0];
                    int prio = atoi(ultima_syscall->args[1]);
                    agregar_a_paquete(p, archivo, strlen(archivo) + 1);
                    agregar_a_paquete(p, &prio, sizeof(int));
                    break;
                }
                case S_EXIT:
                    // exit no tiene argumentos adicionales
                    break;
                }
                // liberamos la estructura de la syscall despues de usarla
                liberar_instruccion(ultima_syscall);
                ultima_syscall = NULL;
            }

            // enviamos la respuesta definitiva al scheduler
            enviar_paquete(p, socket_kernel_scheduler);
            eliminar_paquete(p);

            // liberamos el contexto local para evitar memory leaks
            free(ctx);
            log_info(logger, "## pid: %d - turno finalizado correctamente", pid_actual);
        }
    }

    // limpieza de configuracion y logs antes de cerrar
    config_destroy(config);
    log_destroy(logger);
    return 0;
}

void ObtenerConfig(void)
{

    config = config_create("cpu.config");

    if (config == NULL)
    {
        printf("Error al abrir config\n");
        exit(1);
    }

    ip_kernel_memory = config_get_string_value(config, "IP_KERNEL_MEMORY");
    puerto_kernel_memory = config_get_string_value(config, "PUERTO_KERNEL_MEMORY");

    ip_memory_stick = config_get_string_value(config, "IP_MEMORY_STICK");
    puerto_memory_stick = config_get_string_value(config, "PUERTO_MEMORY_STICK");

    ip_kernel_scheduler = config_get_string_value(config, "IP_KERNEL_SCHEDULER");
    puerto_kernel_scheduler = config_get_string_value(config, "PUERTO_KERNEL_SCHEDULER");
    puerto_interrupt = config_get_string_value(config, "PUERTO_INTERRUPT"); // nueva línea hilo
    log_level = log_level_from_string(
        config_get_string_value(config, "LOG_LEVEL"));
}

//*int conectar_con_reintento(char* ip, char* puerto) //lo borramos

// envío struct completo porque memory recibe así, antes enviaba registro por registro
void actualizar_contexto(int socket, int pid, t_contexto *ctx)
{
    t_paquete *paquete = crear_paquete(ACTUALIZAR_CONTEXTO);
    agregar_a_paquete(paquete, &pid, sizeof(int));
    agregar_a_paquete(paquete, &ctx->registros, sizeof(t_registros)); // bloque completo

    enviar_paquete(paquete, socket);
    eliminar_paquete(paquete);

    log_info(logger, "Contexto enviado a Kernel Memory para actualización del PID %u", pid);
}

// estaba hardcodeado
t_contexto *pedir_contexto(int socket, int pid)
{
    log_debug(logger, "Pedi el contexto");
    t_paquete *paquete = crear_paquete(PEDIDO_CONTEXTO);

    agregar_a_paquete(paquete, &pid, sizeof(int));

    enviar_paquete(paquete, socket);
    eliminar_paquete(paquete);

    int cod_op = recibir_operacion(socket); // esto no iria
    // ACA DEBERIA RECIBIR UN PAQUETE
    /*t_list* paquete_recibido = recibir_paquete(socket);
    int cod_op = *(int*) list_get(paquete_recibido,0);*/

    if (cod_op != RESPUESTA_CONTEXTO)
    {
        printf("Error al recibir contexto\n");
        return NULL;
    }

    t_list *lista = recibir_paquete(socket);

    t_contexto *ctx = malloc(sizeof(t_contexto));

    memcpy(&(ctx->registros.PC), list_get(lista, 0), sizeof(uint32_t));
    memcpy(&(ctx->registros.AX), list_get(lista, 1), sizeof(uint8_t));
    memcpy(&(ctx->registros.BX), list_get(lista, 2), sizeof(uint8_t));
    memcpy(&(ctx->registros.CX), list_get(lista, 3), sizeof(uint8_t));
    memcpy(&(ctx->registros.DX), list_get(lista, 4), sizeof(uint8_t));
    memcpy(&(ctx->registros.EAX), list_get(lista, 5), sizeof(uint32_t));
    memcpy(&(ctx->registros.EBX), list_get(lista, 6), sizeof(uint32_t));
    memcpy(&(ctx->registros.ECX), list_get(lista, 7), sizeof(uint32_t));
    memcpy(&(ctx->registros.EDX), list_get(lista, 8), sizeof(uint32_t));
    memcpy(&(ctx->registros.SI), list_get(lista, 9), sizeof(uint32_t));
    memcpy(&(ctx->registros.DI), list_get(lista, 10), sizeof(uint32_t));
    // uint8_t porque son 1 byte segun consigna?
    int cant_segmentos;

    memcpy(&cant_segmentos, list_get(lista,11), sizeof(int));
    ctx->tabla_segmentos = list_create();

    int indice = 12;

    for(int i = 0; i < cant_segmentos; i++)
    {
        t_segmento* seg = malloc(sizeof(t_segmento));

        memcpy(seg, list_get(lista, indice++), sizeof(t_segmento));

        list_add(ctx->tabla_segmentos, seg);

        log_info(logger, "Recibido segmento %u BASE=%u LIMITE=%u", seg->id, seg->base, seg->limite);
    }

    log_info(logger, "Contexto recibido. Cantidad segmentos: %d", cant_segmentos);
    list_destroy_and_destroy_elements(lista, free);

    log_info(logger, "Contexto del PID %u procesado correctamente", pid);
    return ctx;
}

int conectar(char *ip, char *puerto)
{
    int socket_resultado = crear_conexion(ip, puerto);

    if (socket_resultado <= 0)
    {
        // usamos printf porque quizas el logger no se creo todavia
        printf("error conectando a %s:%s\n", ip, puerto);
        return -1;
    }

    printf("conectado exitosamente a %s:%s\n", ip, puerto);
    return socket_resultado;
}

// hilo secundario para recibir pids a interrumpir desde el scheduler
void *escuchar_interrupciones(void *arg)
{
    while (1)
    {
        int pid_interrumpido;
        op_code tipo;
        // también se recibe el opcode
        recv(socket_scheduler_interrupt, &tipo, sizeof(op_code), MSG_WAITALL);
        
        // recibimos el pid que el scheduler quiere desalojar
        int bytes = recv(socket_scheduler_interrupt, &pid_interrumpido, sizeof(int), MSG_WAITALL);

        if (bytes <= 0)
        {
            break; // conexion cerrada
        }

        // guardamos la interrupcion de forma segura con mutex
        pthread_mutex_lock(&mutex_interrupcion);
        hay_interrupcion = true;
        pid_interrupcion = pid_interrumpido;
        pthread_mutex_unlock(&mutex_interrupcion);

        log_info(logger, "## interrupcion %d recibida para pid: %d", tipo, pid_interrumpido);
        //log_info(logger, "## interrupcion recibida para pid: %d", pid_interrumpido);
    }
    return NULL;
}
// -- para MS
void agregar_memory_stick(int id, uint32_t base, uint32_t limite, char* ip, char* puerto, int socket)
{
    t_memory_stick_cpu* ms =
        malloc(sizeof(t_memory_stick_cpu));

    ms->id = id;
    ms->base = base;
    ms->limite = limite;
    ms->socket = socket;
    strcpy(ms->ip, ip);
    strcpy(ms->puerto, puerto);

    pthread_mutex_lock(&mx_memory_sticks);
    list_add(lista_memory_sticks, ms);
    pthread_mutex_unlock(&mx_memory_sticks);

    log_info(logger, "Memory Stick %d agregado (%s:%s) BASE=%u LIMITE=%u", id, ip, puerto, base, limite);
}

t_memory_stick_cpu* buscar_ms_por_base(uint32_t dir_fisica)
{
    t_memory_stick_cpu* encontrado = NULL;
    pthread_mutex_lock(&mx_memory_sticks);
    for (int i = 0; i < list_size(lista_memory_sticks); i++) {
        t_memory_stick_cpu* ms = list_get(lista_memory_sticks, i);
        if (dir_fisica >= ms->base && dir_fisica < ms->base + ms->limite) {
            encontrado = ms;
            break;
        }
    }
    pthread_mutex_unlock(&mx_memory_sticks);
    return encontrado;
}
// simulador para testear el hilo sin depender del scheduler todavia
//void *simular_interrupcion(void *arg) // no lo necesitamos ahora
//ya tenemos interrupciones reales
