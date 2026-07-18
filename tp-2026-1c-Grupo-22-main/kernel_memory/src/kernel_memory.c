#include <utils/utils.h>
#include <pthread.h>
#include <utils/protocolo.h>
#include <utils/contexto.h>

// Varibales de configuración
t_log_level log_level;
t_config *config;
t_log *logger;

pthread_mutex_t mx_lista_procesos = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mx_orden_llegada = PTHREAD_MUTEX_INITIALIZER;

// Variables para conexión
int socket_servidor;
char *puerto_escucha;
int socket_cpu;
int socket_memory_stick;
char *ip_kernel_memory;

// Delay
int memory_delay;
uint32_t compactacion_delay;

// Funciones para conexión
void ObtenerConfig(void);
void iterator(char *value);
void *atender_cliente(void *arg);
// struct para enviar a CPU (creo que va en utils)
typedef struct
{
    uint32_t id;
    uint32_t base;   // para enviar a CPU es identificar MS
    uint32_t limite; // para enviar a CPU es identificar MS
    char ip[50];
    char puerto[20];
} t_info_memory_stick;

// LISTA GLOBAL (Estructura de procesos)
typedef struct
{
    uint32_t pid;
    t_list *lista_instrucciones;
    t_contexto contexto;
    bool proceso_actulizado;
    bool peticion_eliminacion_proceso;
    t_list *bloques_en_swap;
} t_proceso;

t_list *lista_procesos;

// Funciones de cargar contexto

t_list *leer_instrucciones(char *path);
void cargar_proceso(int socket_cliente);

// Funcion de PEDIR_CONTEXTO
t_contexto obtener_contexto(int socket_cliente);
void enviar_contexto(int socket_cliente, t_contexto contexto);
uint32_t obtener_pid(int socket_cliente);
t_proceso *buscar_por_pid(t_list *lista_procesos, uint32_t pid);
bool contiene_pid(void *ptr);

// Funciones de ACTUALIZAR_CONTEXTO
void actualizar_contexto(int socket_cliente);

// Funciones enviar instrucciones
void enviar_instruccion(int socket_cliente);

// Funcione espacio
void enviar_espacio(int socket_cliente);

// Funciones escritura / lectura
void escritura(int socket_cliente);
void lectura(int socket_cliente);

// Función finalizar proceso
void finalizar_proceso(int socket_cliente);

// Funcion y variables segmentos

void crear_segmento(int socket_cliente);
void eliminar_segmento(int socket_cliente);

uint32_t capacidad_memoria_total = 0;
uint32_t memoria_libre_actual = 0;
int tamano_maximo;

t_list *lista_huecos_memoria;
t_list *lista_memory_sticks_conectados;
t_memory_stick *buscar_por_id_memory_stick(t_list *lista_memory_sticks, uint32_t id);
bool contiene_id(void *ptr);

void compactacion();
bool menor_base(void *a, void *b);
void hueco_destroy(void *ptr);
t_memory_stick *buscar_ms_por_base(u_int32_t base, int *posicion);

// SWAP
size_t size_swap;
size_t block_size;
t_bitarray *bitmap;
int buscar_bloque_libre();
void suspender(int socket_cliente);
void desuspender(int socket_cliente);
void *solicitud_lectura_ms(u_int32_t direccion_fisica, u_int32_t tamano);

int main(int argc, char *argv[])
{

    saludar("kernel_memory");


    if (argc == 1)
    {
        // log_error(logger, "Falta cargar el archivo de configuración. Ejecutar asi: ./kernel_memory kernel_memory.config");
        printf("Uso correcto: ./bin/kernel_memory [Archivo Config] \n");

        exit(EXIT_FAILURE);
    }

    config = config_create(argv[1]);

    if (config == NULL)
    {
        printf("No se pudo cargar el config en la ruta: %s\n", argv[1]);
        exit(EXIT_FAILURE);
    }

    ObtenerConfig();
    logger = log_create("Kernel_memory.log", "Servidor", 1, log_level);//probamos en este orden
    // Inicializamos las listas
    lista_procesos = list_create();
    lista_memory_sticks_conectados = list_create();
    lista_huecos_memoria = list_create();

    socket_servidor = iniciar_servidor(puerto_escucha); // Levanta servidor

    if (socket_servidor < 0)
    {
        log_error(logger, "Error al iniciar servidor");
        config_destroy(config);
        exit(EXIT_FAILURE);
    }

    log_info(logger, "Servidor listo para recibir clientes");

    // 🔥 REEMPLAZA TODO lo de socket_cpu, socket_memory_stick y el while viejo
    while (1)
    {

        int cliente_fd = esperar_cliente(socket_servidor); // Acepta conexiones

        if (cliente_fd < 0)
        {
            log_error(logger, "Error al aceptar cliente");
            continue;
        }

        log_info(logger, "Se conectó un cliente");

        pthread_t thread;

        int *fd_ptr = malloc(sizeof(int));
        *fd_ptr = cliente_fd;

        pthread_create(&thread, NULL, atender_cliente, fd_ptr); // Cada cliente un hilo
        pthread_detach(thread);
    }

    // nunca se ejecuta en un servidor real
    log_destroy(logger);
    config_destroy(config);
    list_destroy_and_destroy_elements(lista_procesos, free);
    list_destroy_and_destroy_elements(lista_memory_sticks_conectados, free);
    list_destroy_and_destroy_elements(lista_huecos_memoria, free);
    bitarray_destroy(bitmap);

    return EXIT_SUCCESS;
}

void iterator(char *value)
{
    log_info(logger, "%s", value);
}

void ObtenerConfig(void)
{

    /* if (config == NULL)
     {
         printf("Error al abrir config\n");
         exit(1);
     }*/

    log_level = log_level_from_string(config_get_string_value(config, "LOG_LEVEL"));
    ip_kernel_memory = config_get_string_value(config, "IP_KERNELMEMORY");
    puerto_escucha = config_get_string_value(config, "PUERTO_KERNEL");
    log_level = log_level_from_string(config_get_string_value(config, "LOG_LEVEL"));
    tamano_maximo = config_get_int_value(config, "SEGMENT_MAX_SIZE");
    // Leemos el delay
    memory_delay = config_get_int_value(config, "INSTRUCTION_DELAY");
    compactacion_delay = config_get_int_value(config, "COMPACTACION_DELAY");
}

t_memory_stick *buscar_por_id_memory_stick(t_list *lista_memory_sticks, uint32_t id)
{
    bool contiene_id(void *ptr)
    {
        // Casteo --> transfroma ptr que es generico a un proceso
        t_memory_stick *memory_stick = (t_memory_stick *)ptr;
        return memory_stick->id == id;
    }
    // pthread_mutex_lock(&mx_lista_procesos); // Bloqueo  --> estaban en la funcion de busca_por_pid, no las borro por las dudas que las tengamos que usar
    t_memory_stick *encontrado = list_find(lista_memory_sticks, contiene_id);
    // pthread_mutex_unlock(&mx_lista_procesos); // Desbloqueo

    return encontrado;
}

int socket_cpu = -1;
int socket_swap = -1;
int socket_kernel_scheduler = -1;
int socket_memory_stick = -1;

// (opcional pero recomendado)
pthread_mutex_t mutex_modulos = PTHREAD_MUTEX_INITIALIZER;

void *atender_cliente(void *arg)
{
    int socket_cliente = *(int *)arg;
    free(arg);

    int tipo;
    recv(socket_cliente, &tipo, sizeof(int), 0);

    pthread_mutex_lock(&mutex_modulos);

    switch (tipo)
    {
    case CPU:
        socket_cpu = socket_cliente;
        int id_cpu;
        recv(socket_cpu, &id_cpu, sizeof(int), MSG_WAITALL);
        log_info(logger, "## CPU: %i Conectada", id_cpu);
        send(socket_cpu, &tamano_maximo, sizeof(tamano_maximo), MSG_WAITALL);

        int cantidad = list_size(lista_memory_sticks_conectados); // antes 2; ahora dinamico

        send(socket_cpu, &cantidad, sizeof(int), 0);
        for (int i = 0;
             i < cantidad;
             i++)
        {
            t_memory_stick *ms =
                list_get(lista_memory_sticks_conectados, i);

            t_info_memory_stick info;

            info.id = ms->id;
            info.base = ms->base; // lo último que agregamos al struct base y limite
            info.limite = ms->limite;

            strcpy(info.ip, ms->ip);

            strcpy(info.puerto, ms->puerto);

            send(socket_cpu, &info, sizeof(info), 0);
        }
        break;

    case SWAP:
        socket_swap = socket_cliente;
        log_info(logger, "Se conectó SWAP");
        recv(socket_swap, &size_swap, sizeof(int), MSG_WAITALL);
        log_info(logger, "TAMAÑO SWAP : %zu", size_swap);
        recv(socket_swap, &block_size, sizeof(int), MSG_WAITALL);
        log_info(logger, "TAMAÑO BLOQUE LOGICO : %zu", block_size);

        // Usamos calloc porque te limpia la memoria antes, tenemos todos los bits en 0

        int total_bloques = size_swap / block_size;
        log_debug(logger, "Total bloque: %i", total_bloques);
        int bytes_necesarios = (total_bloques + 7) / 8;

        void *tamano_swap = calloc(bytes_necesarios, 1);
        bitmap = bitarray_create_with_mode(tamano_swap, bytes_necesarios, LSB_FIRST);

        pthread_mutex_unlock(&mutex_modulos);
        return NULL;
        break;

    case KERNEL_SCHEDULER:
        socket_kernel_scheduler = socket_cliente;
        log_info(logger, "## Kernel Scheduler Conectado - FD del socket: %i", socket_kernel_scheduler);
        break;

    case MEMORY_STICK:
        socket_memory_stick = socket_cliente;
        log_info(logger, "Se conectó el Memory Stick %i", socket_memory_stick);

        // Recibo el tamaño de memory stick
        uint32_t tamano_recibido;
        recv(socket_memory_stick, &tamano_recibido, sizeof(tamano_recibido), MSG_WAITALL);

        char puerto_recibido[20]; // recibimos puerto de MS
        recv(socket_memory_stick, puerto_recibido, sizeof(puerto_recibido), MSG_WAITALL);

        log_info(logger, "Tamaño de Memory stick - %i: %u", socket_memory_stick, tamano_recibido);
        capacidad_memoria_total += tamano_recibido;
        log_info(logger, "Capacidad total de la memoria : %u", capacidad_memoria_total);
        memoria_libre_actual += tamano_recibido;
        log_info(logger, "Memoria libre actual- %i:", memoria_libre_actual);
        // Agrego a lista de los memorys sticks conectados
        t_memory_stick *ms_nuevo = malloc(sizeof(t_memory_stick));
        ms_nuevo->id = socket_memory_stick;
        ms_nuevo->limite = tamano_recibido;

        ms_nuevo->socket = socket_cliente; // probando
        strcpy(ms_nuevo->ip, "127.0.0.1");
        // strcpy(ms_nuevo->puerto, "8001");
        strcpy(ms_nuevo->puerto, puerto_recibido); // ahora dinamico
        // si la lista esta vacio --> la base es 0, sino la base es la base + tamno del anterior
        if (list_is_empty(lista_memory_sticks_conectados))
        {
            ms_nuevo->base = 0;
        }
        else
        {
            t_memory_stick *ultimo_registro = (t_memory_stick *)list_get(lista_memory_sticks_conectados, list_size(lista_memory_sticks_conectados) - 1);
            ms_nuevo->base = ultimo_registro->base + ultimo_registro->limite;
        }
        list_add(lista_memory_sticks_conectados, (t_memory_stick *)ms_nuevo);
        log_info(logger, "MS agregado. Cantidad=%d", list_size(lista_memory_sticks_conectados)); // nuevo
        // Agrego todo el ms a la lista de huecos
        t_hueco *hueco_nuevo = malloc(sizeof(t_hueco));
        hueco_nuevo->id = socket_memory_stick;
        hueco_nuevo->base = ms_nuevo->base;
        hueco_nuevo->limite = ms_nuevo->limite;
        list_add(lista_huecos_memoria, (t_hueco *)hueco_nuevo);

        // Le aviso a Kernel Scherduler que se amplio el espacio
        int aviso = NUEVO_MEMORY_STICK;
        send(socket_kernel_scheduler, &aviso, sizeof(aviso), MSG_WAITALL);

        break;

    default:
        log_warning(logger, "Modulo desconocido");
        close(socket_cliente);
        pthread_mutex_unlock(&mutex_modulos);
        return NULL;
    }

    pthread_mutex_unlock(&mutex_modulos);

    // 🔁 Loop de recepción
    while (1)
    {

        int cod_op = recibir_operacion(socket_cliente);

        // Mostrar codigo recibido

        log_info(logger, "Codigo operacion recibido: %d", cod_op);

        if (cod_op == -1)
        {
            int posicion = 0;
            bool encontrado = false;
            t_memory_stick *memory_encontrado = NULL;
            int tamano_lista_ms = list_size(lista_memory_sticks_conectados);
            while (!encontrado && posicion < tamano_lista_ms)
            {
                memory_encontrado = (t_memory_stick *)list_get(lista_memory_sticks_conectados, posicion);

                if (memory_encontrado->id == socket_cliente)
                {
                    encontrado = true;
                }
                else
                {
                    posicion++;
                }
            }

            if (memory_encontrado != NULL)
            {
                log_warning(logger, "Se desconecto el memory stick con id: %i", socket_cliente);
                int aviso = MEMORIA_CORRUPTA;
                send(socket_kernel_scheduler, &aviso, sizeof(aviso), MSG_WAITALL); // Le aviso a kernel_scheduler
                // Actualizo la memoria total
                capacidad_memoria_total -= memory_encontrado->limite;
                // Actualizo los huecos y la memoria libre
                if (lista_huecos_memoria != NULL)
                {
                    for (int i = 0; i < list_size(lista_huecos_memoria); i++)
                    {
                        t_hueco *hueco_actual = (t_hueco *)list_get(lista_huecos_memoria, i);
                        if (hueco_actual->id == memory_encontrado->id)
                        {
                            memoria_libre_actual -= memory_encontrado->limite;
                            list_remove_element(lista_huecos_memoria, hueco_actual);
                            free(hueco_actual);
                        }
                    }
                }
                // Elimino el memory stick de la lista

                // Actualizo la base del memory stick siguiente
                if (posicion != tamano_lista_ms - 1) //
                {
                    t_memory_stick *memory_anterior;
                    for (int i = posicion; i < tamano_lista_ms - 2; i++)
                    {

                        t_memory_stick *memory_siguiente = (t_memory_stick *)list_get(lista_memory_sticks_conectados, i + 1);
                        if (i == 0) // Si borramos el primer elemento la base del siguiente=0
                        {
                            memory_siguiente->base = 0;
                            memory_anterior = memory_siguiente;
                        }
                        else
                        {
                            memory_anterior = (t_memory_stick *)list_get(lista_memory_sticks_conectados, i - 1);
                            memory_siguiente->base = memory_anterior->base + memory_anterior->limite;
                        }
                    }
                }

                list_remove_element(lista_memory_sticks_conectados, memory_encontrado);
                free(memory_encontrado);
            }
            else
            {
                log_warning(logger, "Cliente desconectado");
            }

            close(socket_cliente);
            return NULL;
        }

        switch (cod_op)
        {
        case MENSAJE:
        {
            recibir_mensaje(socket_cliente);
            break;
        }
        case PAQUETE:
        {
            t_list *lista = recibir_paquete(socket_cliente);
            list_iterate(lista, (void *)iterator);
            // list_destroy(lista);
            break;
        }
        case CARGA_PROCESO:
        {
            cargar_proceso(socket_cliente);
            break;
        }

        case PEDIDO_CONTEXTO:
        {
            t_contexto contexto = obtener_contexto(socket_cliente);
            enviar_contexto(socket_cliente, contexto);
            break;
        }
        case ACTUALIZAR_CONTEXTO:
        {
            actualizar_contexto(socket_cliente);
            break;
        }
        case FETCH_INSTRUCCION:
        {
            enviar_instruccion(socket_cliente);
            break;
        }
        case CONSULTAR_ESPACIO:
        {
            enviar_espacio(socket_cliente);
            break;
        }
        case SOLICITUD_ESCRITURA:
        {
            escritura(socket_cliente);
            break;
        }
        case SOLICITUD_LECTURA:
        {
            lectura(socket_cliente);
            break;
        }
        case FINALIZAR_PROCESO:
        {
            finalizar_proceso(socket_cliente);
            break;
        }
        case CREAR_SEGMENTO:
        {
            crear_segmento(socket_cliente);
            break;
        }
        case ELIMINAR_SEGMENTO:
        {
            eliminar_segmento(socket_cliente);
            break;
        }
        case SUSPENDER_PROCESO:
        {
            suspender(socket_cliente);
            break;
        }
        case DESUSPENDER_PROCESO:
        {
            desuspender(socket_cliente);
            break;
        }

        default:
            log_warning(logger, "Operacion desconocida");
            close(socket_cliente);
            return NULL;
            break;
        }
    }
}

// Cuando se usa hay que chequear que lo que devuelve sea >0
/*uint32_t obtener_pid(int socket_cliente)
{
    uint32_t pid = 0;
    ssize_t bytes_recibido = recv(socket_cliente, &pid, sizeof(uint32_t), MSG_WAITALL);
    if (bytes_recibido < 0)
    {
        log_error(logger, "Error al recibir PID de %i", socket_cliente);
        pid = 0;
    }
    else if (bytes_recibido == 0)
    {
        log_info(logger, "Se desconecto el cliente");
        pid = 0;
    }
    return pid;
}*/
// CREO QUE NO NOS SIEVRE MAS

t_proceso *buscar_por_pid(t_list *lista_procesos, uint32_t pid)
{
    bool contiene_pid(void *ptr)
    {
        // Casteo --> transfroma ptr que es generico a un proceso
        t_proceso *proceso = (t_proceso *)ptr;
        return proceso->pid == pid;
    }
    // pthread_mutex_lock(&mx_orden_llegada); // Bloqueo
    t_proceso *encontrado = list_find(lista_procesos, contiene_pid);
    // pthread_mutex_unlock(&mx_orden_llegada); // Desbloqueo

    return encontrado;
}

// Funciones de PEDIDO_CONTEXTO
t_contexto obtener_contexto(int socket_cliente)
{
    // Creamos un contexto vacio para que no rompa cuando tiene que salir porque algo salio mal
    t_contexto contexto_vacio = {0};
    // Obtenemos el PID con el que vamos a buscar en la lisa de procesos
    // uint32_t pid = obtener_pid(socket_cliente); CREO QUE NO NOS SIRVE MAS
    t_list *paquete_recibido = recibir_paquete(socket_cliente);
    u_int32_t pid = *(u_int32_t *)list_get(paquete_recibido, 0);

    if (paquete_recibido == NULL)
    {
        log_error(logger, "Error al recibir el paquete %u", pid);
        list_destroy_and_destroy_elements(paquete_recibido, free);
        return contexto_vacio;
    }
    // Buscamos en la lista de procesos
    pthread_mutex_lock(&mx_orden_llegada);
    t_proceso *proceso_encontrado = buscar_por_pid(lista_procesos, pid);

    if (proceso_encontrado == NULL)
    {
        log_error(logger, "No se pudo encontrar el contexto del pid %u", pid);

        pthread_mutex_unlock(&mx_orden_llegada);
        list_destroy_and_destroy_elements(paquete_recibido, free);
        return contexto_vacio;
    }

    // agregado
    proceso_encontrado->proceso_actulizado = false;

    pthread_mutex_unlock(&mx_orden_llegada);
    list_destroy_and_destroy_elements(paquete_recibido, free);
    return proceso_encontrado->contexto;
}

void enviar_contexto(int socket_cliente, t_contexto contexto)
{
    // empaquetar el contexto

    t_paquete *paquete_contexto = crear_paquete(RESPUESTA_CONTEXTO);
    agregar_a_paquete(paquete_contexto, &contexto.registros.PC, sizeof(u_int32_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.AX, sizeof(u_int8_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.BX, sizeof(u_int8_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.CX, sizeof(u_int8_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.DX, sizeof(u_int8_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.EAX, sizeof(u_int32_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.EBX, sizeof(u_int32_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.ECX, sizeof(u_int32_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.EDX, sizeof(u_int32_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.SI, sizeof(u_int32_t));
    agregar_a_paquete(paquete_contexto, &contexto.registros.DI, sizeof(u_int32_t));

    if (contexto.tabla_segmentos == NULL)
    {
        log_error(logger, "No existe la tabla de segmentos");
        eliminar_paquete(paquete_contexto);
        return;
    }

    int cant_segmentos = list_size(contexto.tabla_segmentos);
    log_info(logger, "Cantidad de segmentos: %d", cant_segmentos);
    agregar_a_paquete(paquete_contexto, &cant_segmentos, sizeof(int));

    for (int i = 0; i < cant_segmentos; i++)
    {
        t_segmento *segmento = (t_segmento *)list_get(contexto.tabla_segmentos, i);

        agregar_a_paquete(paquete_contexto, segmento, sizeof(t_segmento));
        log_info(logger, "Enviando segmento %u BASE=%u LIMITE=%u", segmento->id, segmento->base, segmento->limite);
    }
    log_info(logger, "Enviando contexto con %d segmentos", cant_segmentos);

    enviar_paquete(paquete_contexto, socket_cliente);
    eliminar_paquete(paquete_contexto);
    //int respuesta = OK;
    //send(socket_cliente, &respuesta, sizeof(respuesta), MSG_WAITALL);
}

// Funciones de CARGA_CONTEXTO

void cargar_proceso(int socket_cliente)
{
    // uint32_t pid = obtener_pid(socket_cliente); CREO QUE NO NOS SIRVE MAS
    t_list *paquete_recibido = recibir_paquete(socket_cliente);

    // Muestro lo que recibe km
    if (paquete_recibido == NULL)
    {
        log_info(logger, "Error al recibir el proceso para cargar");
        t_paquete *paquete_respuesta_ks = crear_paquete(OK);
        // agregar_a_paquete(paquete_respuesta_ks, &pid);
        enviar_paquete(paquete_respuesta_ks, socket_cliente);
        eliminar_paquete(paquete_respuesta_ks);
        exit(EXIT_FAILURE);
    }

    u_int32_t pid = *(u_int32_t *)list_get(paquete_recibido, 0);
    // u_int32_t size_path = *(u_int32_t *)list_get(paquete_recibido, 1);
    char *nombre_path = (char *)list_get(paquete_recibido, 2);

    log_info(logger, "## Recibi proceso PID: %d - Archivo: %s", pid, nombre_path);

    /*// recibir el tamaño del path del archivo
    uint32_t size_path;
    recv(socket_cliente, &size_path, sizeof(uint32_t), MSG_WAITALL);

    // recibir el nombre del path del archivo
    char *nombre_path = malloc(size_path);
    recv(socket_cliente, nombre_path, size_path, MSG_WAITALL); */

    // armamos path completo
    // ESTO ASUMO QUE SE DEBERIA CAMBIAR
    char *carpeta = config_get_string_value(config, "SCRIPTS_BASEPATH");
    // char *extension = ".txt";

    int tamaño_path_completo = strlen(carpeta) + strlen(nombre_path) + 2; // El +2 es por los simbolos que no se ven pero estan como \0
    char *path_completo = malloc(tamaño_path_completo);
    sprintf(path_completo, "%s/%s", carpeta, nombre_path); // los simbolos raros son para que concatene, significa string(carpeta) separador nombre del archivo y extensión

    t_list *instrucciones = leer_instrucciones(path_completo);

    if (instrucciones == NULL || list_is_empty(instrucciones))
    {
        log_error(logger, "## PID: %d - ERROR: No se encontró el archivo en %s", pid, path_completo);
        /*int respuesta = ERROR;
        send(socket_cliente, &respuesta, sizeof(respuesta), MSG_WAITALL);*/
        t_paquete *paquete_respuesta_ks = crear_paquete(ERROR);
        // agregar_a_paquete(paquete_respuesta_ks, &pid);
        enviar_paquete(paquete_respuesta_ks, socket_cliente);
        eliminar_paquete(paquete_respuesta_ks);
        // Limpiamos y salimos de la función ANTES de crear el proceso
        free(path_completo);
        // if (instrucciones != NULL)
        list_destroy(instrucciones);
        list_destroy_and_destroy_elements(paquete_recibido, free);
        return;
    }

    // Verificamos
    pthread_mutex_lock(&mx_orden_llegada);
    // log_debug(logger, "Mutex bloqueado");
    t_proceso *existente = buscar_por_pid(lista_procesos, pid);

    if (existente != NULL)
    {
        log_error(logger, "PID %d ya existe en memoria", pid);
        pthread_mutex_unlock(&mx_orden_llegada);
        // log_debug(logger, "Mutex desbloqueado");
        free(path_completo);
        list_destroy_and_destroy_elements(paquete_recibido, free);
        list_destroy_and_destroy_elements(instrucciones, free);

        return;
    }

    // Si llegamos acá, el archivo existe y tiene contenido
    t_proceso *nuevo = malloc(sizeof(t_proceso));
    nuevo->pid = pid;
    nuevo->lista_instrucciones = instrucciones;

    // inicializamos contexto
    incializar_contexto(&nuevo->contexto);

    // Inicializamos tabla de segmentos
    nuevo->contexto.tabla_segmentos = list_create();

    nuevo->peticion_eliminacion_proceso = false;
    nuevo->proceso_actulizado = false;
    nuevo->bloques_en_swap = list_create();

    list_add(lista_procesos, nuevo);
    pthread_mutex_unlock(&mx_orden_llegada);

    log_info(logger, "## PID: %d - PROCESO CREADO CORRECTAMENTE", pid);
    op_code ok = OK;
    send(socket_cliente, &ok, sizeof(op_code), 0); // le mandamos el ok sin que sea un paquete
    log_info(logger, "OK enviado");

    /*
    t_paquete *paquete_respuesta_ks = crear_paquete(OK);
    // agregar_a_paquete(paquete_respuesta_ks, &pid);
    enviar_paquete(paquete_respuesta_ks, socket_cliente);
    eliminar_paquete(paquete_respuesta_ks);*/
    /*int respuesta = OK;
    send(socket_cliente, &respuesta, sizeof(respuesta), MSG_WAITALL);
    list_destroy_and_destroy_elements(paquete_recibido, free);*/

    free(path_completo);
    // Muestro el registro del proceso creado
    /*t_proceso *proceso_creado = buscar_por_pid(lista_procesos, pid);
    if (proceso_creado != NULL)
    {
        log_info(logger, "El pid del proceso es: %i", proceso_creado->pid);
        log_info(logger, "los registro estan asi: %i\n %i\n %i\n %i\n", proceso_creado->contexto.registros.AX, proceso_creado->contexto.registros.BX, proceso_creado->contexto.registros.CX, proceso_creado->contexto.registros.DX);

    }*/
}

// Función de actualizar contexto

void actualizar_contexto(int socket_cliente)
{

    t_list *paquete_recibido = recibir_paquete(socket_cliente);

    if (paquete_recibido == NULL)
    {
        log_error(logger, "Error en el recibo de la respuesta del cliente %i:", socket_cliente);
        return;
    }

    u_int32_t pid = *(u_int32_t *)list_get(paquete_recibido, 0);
    t_registros registros_actualizados = *(t_registros *)list_get(paquete_recibido, 1);

    pthread_mutex_lock(&mx_orden_llegada);
    // log_debug(logger, "Mutex bloqueado");
    t_proceso *proceso_a_actualizar = buscar_por_pid(lista_procesos, pid);

    if (proceso_a_actualizar == NULL)
    {
        log_error(logger, "## PID: %d - No se pudo actualizar: Proceso no encontrado", pid);
        pthread_mutex_unlock(&mx_orden_llegada);
        exit(EXIT_FAILURE);
    }

    // Como buscar_por_pid ya tiene mutex, aquí recibimos el puntero seguro
    proceso_a_actualizar->contexto.registros = registros_actualizados;
    log_info(logger, "## PID: %d - Contexto actualizado", pid);
    log_info(logger, "PC=%u AX=%u BX=%u CX=%u DX=%u",
             registros_actualizados.PC,
             registros_actualizados.AX,
             registros_actualizados.BX,
             registros_actualizados.CX,
             registros_actualizados.DX);
    log_info(logger, "EAX=%u EBX=%u ECX=%u EDX=%u SI=%u DI=%u",
             registros_actualizados.EAX,
             registros_actualizados.EBX,
             registros_actualizados.ECX,
             registros_actualizados.EDX,
             registros_actualizados.SI,
             registros_actualizados.DI);

    proceso_a_actualizar->proceso_actulizado = true;

    if (proceso_a_actualizar->peticion_eliminacion_proceso == true)
    {
        list_remove_element(lista_procesos, proceso_a_actualizar); // Elimino de la lista
        free(proceso_a_actualizar);                                // Elimino de memoria
        log_info(logger, "Se eliminó el proceso con pid %i correctamente ", pid);
    }
    /*else
    {
        log_error(logger, "## PID: %d - No se pudo actualizar: Proceso no encontrado", pid);
    }*/
    pthread_mutex_unlock(&mx_orden_llegada);
    // log_debug(logger, "Mutex desbloqueado");
    list_destroy_and_destroy_elements(paquete_recibido, free);
    //int respuesta = OK;
    //send(socket_cliente, &respuesta, sizeof(respuesta), MSG_WAITALL);
}

// FUNCION LEER INSTRUCCIONES

t_list *leer_instrucciones(char *path)
{
    FILE *archivo = fopen(path, "r");

    if (archivo == NULL)
    {
        log_error(logger, "No se pudo abrir el archivo: %s", path);
        return list_create(); // lista vacia
    }

    t_list *lista = list_create();
    char *linea = NULL;
    size_t len = 0;

    while (getline(&linea, &len, archivo) != -1)
    {
        // eliminamos salto del linea
        linea[strcspn(linea, "\n")] = '\0';

        // duplicamos string
        char *instruccion = strdup(linea);

        list_add(lista, instruccion);
    }

    free(linea);
    fclose(archivo);

    return lista;
}

// Funciones de Fetch

void enviar_instruccion(int socket_cliente)
{
    // log_debug(logger, "Entre a enviar instruccion");

    // t_fetch paquete_respuesta;
    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        return;
    }

    // Obtener pid del paquete recibido
    u_int32_t pid_recibido = *(uint32_t *)list_get(paquete_respuesta, 0);
    u_int32_t pc_recibido = *(uint32_t *)list_get(paquete_respuesta, 1);

    // Obtener el proceso de la lista de procesos
    pthread_mutex_lock(&mx_orden_llegada);
    // log_debug(logger, "Mutex bloqueado");
    t_proceso *proceso_obtenido = buscar_por_pid(lista_procesos, pid_recibido);

    if (proceso_obtenido == NULL)
    {
        pthread_mutex_unlock(&mx_orden_llegada);

        log_error(logger, "No existe el proceso para el pid: %d", pid_recibido);
        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    if (pc_recibido >= list_size(proceso_obtenido->lista_instrucciones))
    {

        log_error(logger, "PC fuera de rango para PID %d", pid_recibido);
        pthread_mutex_unlock(&mx_orden_llegada);

        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    // Obtener la instruccion segun el PC de la lista de instruccion de proceso_obtenido
    char *instruccion = (char *)list_get(proceso_obtenido->lista_instrucciones, pc_recibido);

    if (instruccion == NULL)
    {
        log_error(logger, "No hay más instrucciones para el PC %d", pc_recibido);
        pthread_mutex_unlock(&mx_orden_llegada);

        return;
    }

    log_info(logger, "## PID: %i - Obtener instrucción:%i - Instrucción: %s ", pid_recibido, pc_recibido, instruccion);

    // retardo de acceso a memoria, delay
    usleep(memory_delay * 1000);
    pthread_mutex_unlock(&mx_orden_llegada);

    // 📦 Envío limpio alineado con la CPU:
    t_paquete *paquete_instruccion = crear_paquete(RESPUESTA_INSTRUCCION);
    agregar_a_paquete(paquete_instruccion, instruccion, strlen(instruccion) + 1);
    
    // Mandamos el paquete directo. La CPU primero leerá el op_code con recibir_operacion
    // y luego el contenido con recibir_paquete.
    enviar_paquete(paquete_instruccion, socket_cliente);
    eliminar_paquete(paquete_instruccion);

    log_info(logger, "## PID: %i PC obtenido: %i. Se envio la instruccion %s ", pid_recibido, pc_recibido, instruccion);
    list_destroy_and_destroy_elements(paquete_respuesta, free);
}

// CONCULTAR ESPACIO

void enviar_espacio(int socket_cliente)
{

    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        return;
    }

    // Obtener pid del paquete recibido
    u_int32_t pid_recibido = *(uint32_t *)list_get(paquete_respuesta, 0);
    u_int32_t tamaño_recibido = *(uint32_t *)list_get(paquete_respuesta, 1);

    // u_int32_t tamaño_falso = 1024;

    op_code respuesta;
    if (tamaño_recibido > memoria_libre_actual)
    {
        respuesta = MEMORIA_LLENA;
    }
    else
    {
        respuesta = MEMORIA_OK;
    }

    t_paquete *paquete_respuesta_memoria = crear_paquete(respuesta);
    agregar_a_paquete(paquete_respuesta_memoria, &respuesta, sizeof(int));
    agregar_a_paquete(paquete_respuesta_memoria, &pid_recibido, sizeof(u_int32_t));
    agregar_a_paquete(paquete_respuesta_memoria, &tamaño_recibido, sizeof(u_int32_t)); // NO SE QUE TAN UTIL ES MANDARLE ESTO
    enviar_paquete(paquete_respuesta_memoria, socket_cliente);
    eliminar_paquete(paquete_respuesta_memoria);

    list_destroy_and_destroy_elements(paquete_respuesta, free);
}

// SOLICITUD_ESCRITURA

void escritura(int socket_cliente)
{
    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        return;
    }

    // Obtener pid y tamaño del paquete recibido
    // u_int32_t pid_recibido = *(uint32_t *)list_get(paquete_respuesta, 0);
    u_int32_t tamaño_recibido = *(uint32_t *)list_get(paquete_respuesta, 1);
    u_int32_t valor = *(uint32_t *)list_get(paquete_respuesta, 2);
    u_int32_t direccion_logica = *(uint32_t *)list_get(paquete_respuesta, 3);

    t_memory_stick *ms = buscar_ms_por_base(direccion_logica, NULL);

    if (ms == NULL)
    {
        log_error(logger, "No se encontró un Memory Stick para la dirección %u", direccion_logica);

        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    int socket_ms = ms->socket;

    uint32_t direccion_local = direccion_logica - ms->base;

    t_paquete *paquete_ms = crear_paquete(SOLICITUD_ESCRITURA_MS);

    agregar_a_paquete(paquete_ms, &direccion_local, sizeof(uint32_t));
    agregar_a_paquete(paquete_ms, &tamaño_recibido, sizeof(uint32_t));
    agregar_a_paquete(paquete_ms, &valor, sizeof(uint32_t));

    enviar_paquete(paquete_ms, socket_ms);
    eliminar_paquete(paquete_ms);

    // Esperamos respuesta de MS
    int respuesta_ms;

    recv(socket_ms, &respuesta_ms, sizeof(int), MSG_WAITALL);

    if (respuesta_ms != OK)
    {
        log_error(logger, "Error en escritura del Memory Stick");

        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    // Respondemos a CPU
    t_paquete *respuesta_cpu = crear_paquete(RESPUESTA_ESCRITURA);

    int ok = OK;

    agregar_a_paquete(respuesta_cpu, &ok, sizeof(int));

    enviar_paquete(respuesta_cpu, socket_cliente);
    eliminar_paquete(respuesta_cpu);

    list_destroy_and_destroy_elements(paquete_respuesta, free);

    /*
    char *respuesta = "ESCRITURA OK";

    t_paquete *paquete_respuesta_escritura = crear_paquete(RESPUESTA_ESCRITURA);
    agregar_a_paquete(paquete_respuesta_escritura, respuesta, strlen(respuesta) + 1);
    enviar_paquete(paquete_respuesta_escritura, socket_cliente);
    eliminar_paquete(paquete_respuesta_escritura);

    list_destroy_and_destroy_elements(paquete_respuesta, free);*/
}

// SOLICITUD_LECTURA
void lectura(int socket_cliente)
{
    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        return;
    }

    // Obtener pid y tamaño del paquete recibido
    // u_int32_t pid_recibido = *(uint32_t *)list_get(paquete_respuesta, 0);
    u_int32_t tamaño_recibido = *(uint32_t *)list_get(paquete_respuesta, 1);
    u_int32_t direccion_logica = *(uint32_t *)list_get(paquete_respuesta, 2);

    t_memory_stick *ms = buscar_ms_por_base(direccion_logica, NULL);

    if (ms == NULL)
    {
        log_error(logger, "No se encontró un Memory Stick para la dirección %u", direccion_logica);

        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    int socket_ms = ms->socket;

    uint32_t direccion_local = direccion_logica - ms->base;

    t_paquete *paquete_ms = crear_paquete(SOLICITUD_LECTURA_MS);

    agregar_a_paquete(paquete_ms, &direccion_local, sizeof(uint32_t));
    agregar_a_paquete(paquete_ms, &tamaño_recibido, sizeof(uint32_t));

    enviar_paquete(paquete_ms, socket_ms);
    eliminar_paquete(paquete_ms);

    int cod_op = recibir_operacion(socket_ms);

    if (cod_op != RESPUESTA_LECTURA_MS)
    {
        log_error(logger, "Error al recibir respuesta de lectura del Memory Stick");

        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    t_list *datos_leidos = recibir_paquete(socket_ms);

    void *contenido = list_get(datos_leidos, 0);

    if (contenido == NULL)
    {
        log_error(logger, "No se recibieron datos del Memory Stick");

        list_destroy_and_destroy_elements(datos_leidos, free);
        list_destroy_and_destroy_elements(paquete_respuesta, free);

        return;
    }

    // Respuesta a cpu

    t_paquete *respuesta_cpu = crear_paquete(RESPUESTA_LECTURA);

    agregar_a_paquete(respuesta_cpu, contenido, tamaño_recibido);

    enviar_paquete(respuesta_cpu, socket_cliente);
    eliminar_paquete(respuesta_cpu);

    list_destroy_and_destroy_elements(datos_leidos, free);
    list_destroy_and_destroy_elements(paquete_respuesta, free);

    /*
    char *respuesta = "LECTURA OK";

    t_paquete *paquete_respuesta_lectura = crear_paquete(RESPUESTA_LECTURA);
    agregar_a_paquete(paquete_respuesta_lectura, respuesta, strlen(respuesta) + 1);
    enviar_paquete(paquete_respuesta_lectura, socket_cliente);
    eliminar_paquete(paquete_respuesta_lectura);

    list_destroy_and_destroy_elements(paquete_respuesta, free); */
}

void finalizar_proceso(int socket_cliente)
{

    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        return;
    }

    u_int32_t pid_recibido = *(uint32_t *)list_get(paquete_respuesta, 0);

    pthread_mutex_lock(&mx_orden_llegada);
    t_proceso *proceso_obtenido = buscar_por_pid(lista_procesos, pid_recibido);

    if (proceso_obtenido == NULL)
    {
        pthread_mutex_unlock(&mx_orden_llegada);
        int respuesta = OK;
        send(socket_cliente, &respuesta, sizeof(respuesta), MSG_WAITALL);
        log_error(logger, "No existe el pdi %i en el lista de procesos:", pid_recibido);
        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    if (proceso_obtenido->proceso_actulizado == false)
    {
        proceso_obtenido->peticion_eliminacion_proceso = true;
        pthread_mutex_unlock(&mx_orden_llegada);
    }
    else
    {
        // Actualizo los huecos
        t_list *tabla_de_segmetos = proceso_obtenido->contexto.tabla_segmentos;

        for (int i = 0; i < list_size(tabla_de_segmetos); i++)
        {
            t_segmento *segmento_obtenido = list_get(tabla_de_segmetos, i);
            memoria_libre_actual += segmento_obtenido->limite;
            t_hueco *hueco_nuevo = malloc(sizeof(t_hueco));
            hueco_nuevo->base = segmento_obtenido->base;
            hueco_nuevo->limite = segmento_obtenido->limite;
            t_memory_stick *ms = buscar_ms_por_base(hueco_nuevo->base, NULL);
            hueco_nuevo->id = ms->id;
            list_add(lista_huecos_memoria, hueco_nuevo);
        }
        // Elimino las estructuras
        list_remove_element(lista_procesos, proceso_obtenido);
        list_destroy_and_destroy_elements(proceso_obtenido->lista_instrucciones, free);
        list_destroy_and_destroy_elements(proceso_obtenido->contexto.tabla_segmentos, free);
        free(proceso_obtenido);
        pthread_mutex_unlock(&mx_orden_llegada);
        log_info(logger, "Se eliminó el proceso con pid %i correctamente ", pid_recibido);
    }

    list_destroy_and_destroy_elements(paquete_respuesta, free);
    int respuesta = OK;
    send(socket_cliente, &respuesta, sizeof(respuesta), MSG_WAITALL);
}

void crear_segmento(int socket_cliente)
{

    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        return;
    }
    uint32_t pid = *(uint32_t *)list_get(paquete_respuesta, 0);
    uint32_t id_segmento = *(uint32_t *)list_get(paquete_respuesta, 1);
    uint32_t tamano_segmento = *(uint32_t *)list_get(paquete_respuesta, 2);

    if (tamano_segmento > tamano_maximo || capacidad_memoria_total < tamano_segmento || memoria_libre_actual < tamano_segmento)
    {
        log_error(logger, "No es posible crear el segmento del pid: %u con id %u", pid, id_segmento);
        t_paquete *paquete = crear_paquete(ERROR_CREACION_SEGMENTO);
        agregar_a_paquete(paquete, &pid, sizeof(uint32_t));
        agregar_a_paquete(paquete, &id_segmento, sizeof(uint32_t));
        enviar_paquete(paquete, socket_kernel_scheduler);
        eliminar_paquete(paquete);
        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    // busco la tabla de segmento del pid
    t_proceso *proceso_obtenido = buscar_por_pid(lista_procesos, pid);
    t_list *tabla_de_segmentos = proceso_obtenido->contexto.tabla_segmentos;

    t_segmento *segmento_nuevo = malloc(sizeof(t_segmento));
    segmento_nuevo->id = id_segmento;
    segmento_nuevo->limite = tamano_segmento;

    char *estrategia_de_asignacion = config_get_string_value(config, "ALLOCATION_STRATEGY");

    log_info(logger, "===== HUECOS ANTES DE MEM_ALLOC =====");

    for (int i = 0; i < list_size(lista_huecos_memoria); i++)
    {
        t_hueco *h = list_get(lista_huecos_memoria, i);

        log_info(logger,
                "Hueco %d -> MS=%d BASE=%u LIMITE=%u",
                i,
                h->id,
                h->base,
                h->limite);
    }

    t_hueco *hueco_elegido = NULL;
    if (strcmp(estrategia_de_asignacion, "BEST") == 0)
    {
        for (int i = 0; i < list_size(lista_huecos_memoria); i++)
        {
            t_hueco *hueco_actual = (t_hueco *)list_get(lista_huecos_memoria, i);

            if (hueco_actual->limite >= tamano_segmento && (hueco_elegido == NULL || hueco_actual->limite < hueco_elegido->limite))
            {
                hueco_elegido = hueco_actual;
            }
        }
    }
    else if (strcmp(estrategia_de_asignacion, "WORST") == 0)
    {
        // Agarro el hueco mas grande
        for (int i = 0; i < list_size(lista_huecos_memoria); i++)
        {
            t_hueco *hueco_actual = (t_hueco *)list_get(lista_huecos_memoria, i);

            if (hueco_actual->limite >= tamano_segmento && (hueco_elegido == NULL || hueco_actual->limite > hueco_elegido->limite))
            {
                hueco_elegido = hueco_actual;
            }
        }
    }
    else
    {
        log_info(logger, "No es una estrategia válida");
        free(segmento_nuevo);
        list_destroy_and_destroy_elements(paquete_respuesta, free);
        exit(EXIT_FAILURE);
    }

    if (hueco_elegido == NULL) // Si llega aca es porque hay memoria disponible pero no contigua
    {
        // Avisarle a ks que hay que hacer una compactacion
        int tipo = AVISO_COMPACTACION;
        send(socket_kernel_scheduler, &tipo, sizeof(tipo), MSG_WAITALL);

        // Esperar confirmacion
        int respuesta;
        recv(socket_kernel_scheduler, &respuesta, sizeof(respuesta), MSG_WAITALL);

        // iniciar la compactacion
        compactacion();
        log_info(logger, "Se realizo una compactación");

        log_info(logger, "Cantidad de huecos luego de compactar: %d", list_size(lista_huecos_memoria));

        for(int i = 0; i < list_size(lista_huecos_memoria); i++)
        {
            t_hueco *h= list_get(lista_huecos_memoria, i);

            log_info(logger, "Hueco %d -> BASE=%u LIMITE=%u", i, h->base, h->limite);
        }

        if (strcmp(estrategia_de_asignacion, "BEST") == 0)
        {
            for (int i = 0; i < list_size(lista_huecos_memoria); i++)
            {
                t_hueco *hueco_actual = (t_hueco *)list_get(lista_huecos_memoria, i);

                if (hueco_actual->limite >= tamano_segmento && (hueco_elegido == NULL || hueco_actual->limite < hueco_elegido->limite))
                {
                    hueco_elegido = hueco_actual;
                }
            }
        }
        else if (strcmp(estrategia_de_asignacion, "WORST") == 0)
        {
            // Agarro el hueco mas grande
            for (int i = 0; i < list_size(lista_huecos_memoria); i++)
            {
                t_hueco *hueco_actual = (t_hueco *)list_get(lista_huecos_memoria, i);

                if (hueco_actual->limite >= tamano_segmento && (hueco_elegido == NULL || hueco_actual->limite > hueco_elegido->limite))
                {
                    hueco_elegido = hueco_actual;
                }
            }
        }
        else
        {
            log_info(logger, "No es una estrategia válida");
            free(segmento_nuevo);
            list_destroy_and_destroy_elements(paquete_respuesta, free);
            exit(EXIT_FAILURE);
        }

        if(hueco_elegido == NULL)
        {
            log_error(logger, "ERROR: despues de compactar no se encontro un hueco para %u bytes", tamano_segmento);
            free(segmento_nuevo);
            list_destroy_and_destroy_elements(paquete_respuesta, free);
            return;
        }
    
    }

    //Compruebo
    if(hueco_elegido == NULL)
    {
        log_error(logger, "FATAL: hueco_elegido es NULL antes de asignar la base");
        exit(EXIT_FAILURE);
    }

    // Agrego a la lista
    segmento_nuevo->base = hueco_elegido->base;
    list_add(tabla_de_segmentos, segmento_nuevo);

    // Actulizo la lista de huecos
    if (tamano_segmento == hueco_elegido->limite) // si se ocupo todo lo borro completo
    {
        list_remove_element(lista_huecos_memoria, hueco_elegido);
        free(hueco_elegido);
    }
    else // si no se borra todo, lo actualizo
    {
        hueco_elegido->base += segmento_nuevo->limite;
        hueco_elegido->limite -= tamano_segmento;
    }

    log_info(logger, "===== HUECOS DESPUES DE MEM_ALLOC =====");

    for (int i = 0; i < list_size(lista_huecos_memoria); i++)
    {
        t_hueco *h = list_get(lista_huecos_memoria, i);

        log_info(logger,
                "Hueco %d -> MS=%d BASE=%u LIMITE=%u",
                i,
                h->id,
                h->base,
                h->limite);
    }

    // Actualizo variable global de memoria
    memoria_libre_actual -= tamano_segmento;

    t_paquete *paquete = crear_paquete(SEGMENTO_CREADO_CORRECTAMENTE);
    agregar_a_paquete(paquete, &pid, sizeof(uint32_t));
    agregar_a_paquete(paquete, &id_segmento, sizeof(uint32_t));
    enviar_paquete(paquete, socket_kernel_scheduler);
    eliminar_paquete(paquete);
    log_info(logger, "## PID: <%u> - Segmento Creado <%u> - Tamaño: <%u>", pid, id_segmento, tamano_segmento);
    list_destroy_and_destroy_elements(paquete_respuesta, free);
}

bool menor_base(void *a, void *b)
{
    t_segmento *segmento_a = (t_segmento *)a;
    t_segmento *segmento_b = (t_segmento *)b;
    return segmento_a->base < segmento_b->base;
}

void hueco_destroy(void *ptr)
{
    t_hueco *hueco = (t_hueco *)ptr;
    /*free(hueco->base);
    free(hueco->id);
    free(hueco->tamano);*/
    free(hueco);
}
// Devulve la posicion del memory stick en la lista de ms en la variable. Y el ms encontrado o NULL
t_memory_stick *buscar_ms_por_base(uint32_t base, int *posicion)
{
    t_memory_stick *ms_encontrado = NULL;

    for (int i = 0; i < list_size(lista_memory_sticks_conectados); i++)
    {
        ms_encontrado = list_get(lista_memory_sticks_conectados, i);

        if (base >= ms_encontrado->base &&
            base < ms_encontrado->base + ms_encontrado->limite)
        {
            if (posicion != NULL)
            {
                *posicion = i;
            }

            return ms_encontrado;
        }
    }

    return NULL;
}

void compactacion()
{
    log_info(logger, "## Iniciando compactacion");



    t_list *tabla_de_segmentos_global = list_create();
    // Recorre todos los proceso
    for (int i = 0; i < list_size(lista_procesos); i++)
    {
        // Agrego todos los segmentos a una lista de segmentos global
        t_proceso *proceso = list_get(lista_procesos, i);
        list_add_all(tabla_de_segmentos_global, proceso->contexto.tabla_segmentos);
    }

    // Ordeno por la base de forma ascendente
    t_list *tabla_de_segmentos_global_ordenada = NULL;
    tabla_de_segmentos_global_ordenada = list_sorted(tabla_de_segmentos_global, menor_base);

    //Limpio los huecos viejos porque la memoria va a cambiar completamente
    list_clean_and_destroy_elements(lista_huecos_memoria, hueco_destroy);

    // Chequeo de si hay huecos en la lista de segmentos
    if (list_size(tabla_de_segmentos_global_ordenada) > 0)
    {
        t_segmento *primer_segmento = list_get(tabla_de_segmentos_global_ordenada, 0);
        if (primer_segmento->base != 0)
        {
            primer_segmento->base = 0;
        }
    }

    for (int i = 0; i < list_size(tabla_de_segmentos_global_ordenada); i++)
    {

        t_segmento *segmento_actual = list_get(tabla_de_segmentos_global_ordenada, i);
        t_segmento *segmento_siguiente = NULL;

        if (i != list_size(tabla_de_segmentos_global_ordenada) - 1) // Chequeo que no sea el ultimo segmento
        {
            segmento_siguiente = list_get(tabla_de_segmentos_global_ordenada, i + 1);
        }

        if (segmento_siguiente == NULL) // Es el ultimo segemnto de la lista
        {

            if (segmento_actual->base + segmento_actual->limite != capacidad_memoria_total)
            {
                t_hueco *hueco_nuevo = malloc(sizeof(t_hueco));
                hueco_nuevo->base = segmento_actual->base + segmento_actual->limite;

                // Busco a que MS pertence el hueco
                int posicion;
                t_memory_stick *ms_encontrado = buscar_ms_por_base(hueco_nuevo->base, &posicion);

                hueco_nuevo->id = ms_encontrado->id;
                u_int32_t final_ms = ms_encontrado->base + ms_encontrado->limite;
                hueco_nuevo->limite = final_ms - hueco_nuevo->base;
                list_add(lista_huecos_memoria, hueco_nuevo);

                if (hueco_nuevo->base + hueco_nuevo->limite != capacidad_memoria_total)
                {
                    // Hay mas MS libres
                    for (int j = posicion + 1; j < list_size(lista_memory_sticks_conectados); j++)
                    {
                        t_memory_stick *ms_libre = list_get(lista_memory_sticks_conectados, j);
                        t_hueco *hueco_ms = malloc(sizeof(t_hueco));
                        hueco_ms->id = ms_libre->id;
                        hueco_ms->base = ms_libre->base;
                        hueco_ms->limite = ms_libre->limite;
                        list_add(lista_huecos_memoria, hueco_ms);
                    }
                }
            }
        }
        else
        {
            if (segmento_actual->base + segmento_actual->limite != segmento_siguiente->base)
            {
                segmento_siguiente->base = segmento_actual->base + segmento_actual->limite;
            }
        }
    }

    // Crear el único hueco libre restante luego de compactar
    uint32_t memoria_usada = 0;

    for(int i = 0; i < list_size(tabla_de_segmentos_global_ordenada); i++)
    {
        t_segmento *seg = list_get(tabla_de_segmentos_global_ordenada, i);
        memoria_usada += seg->limite;
    }


    if(memoria_usada < capacidad_memoria_total)
    {
        t_hueco *hueco_final = malloc(sizeof(t_hueco));

        hueco_final->base = memoria_usada;
        hueco_final->limite = capacidad_memoria_total - memoria_usada;

        int posicion;
        t_memory_stick *ms = buscar_ms_por_base(hueco_final->base, &posicion);

        hueco_final->id = ms->id;

        list_add(lista_huecos_memoria, hueco_final);
    }


    usleep(compactacion_delay * 1000);
    log_info(logger, "## Compactacion finalizada");

    list_destroy(tabla_de_segmentos_global);
    list_destroy(tabla_de_segmentos_global_ordenada);
}

t_segmento *buscar_por_id_segmento(t_list *lista_segmentos, u_int32_t id_segmento)
{

    bool contiene_id(void *ptr)
    {
        // Casteo --> transfroma ptr que es generico a un proceso
        t_segmento *segmento = (t_segmento *)ptr;
        return segmento->id == id_segmento;
    }
    // pthread_mutex_lock(&mx_lista_procesos); // Bloqueo
    t_segmento *encontrado = list_find(lista_segmentos, contiene_id);
    // pthread_mutex_unlock(&mx_lista_procesos); // Desbloqueo

    return encontrado;
}

void eliminar_segmento(int socket_cliente)
{

    log_info(logger, "Entre a eliminar_segmento");

    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    log_info(logger, "Paquete recibido");

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        exit(EXIT_FAILURE);
    }
    
    uint32_t pid = *(uint32_t *)list_get(paquete_respuesta, 0);
    
    log_info(logger, "PID=%u", pid);

    uint32_t id_segmento = *(uint32_t *)list_get(paquete_respuesta, 1);

    log_info(logger, "SEG=%u", id_segmento);
    log_info(logger, "Buscando proceso...");
    
    t_proceso *proceso_encontrado = buscar_por_pid(lista_procesos, pid);
    log_info(logger, "Proceso encontrado");

    if (proceso_encontrado == NULL)
    {
        log_error(logger, "No existe el proceso con el pid%u:", pid);
        exit(EXIT_FAILURE);
    }

    t_segmento *segmento_encontrado = buscar_por_id_segmento(proceso_encontrado->contexto.tabla_segmentos, id_segmento);
    log_info(logger, "Buscando segmento...");

    if (segmento_encontrado == NULL)
    {
        log_error(logger, "No existe el segemnto con id %u del pid%u:", id_segmento, pid);
        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    log_info(logger, "Segmento encontrado");

    t_hueco *hueco_nuevo = malloc(sizeof(t_hueco));
    hueco_nuevo->base = segmento_encontrado->base;
    hueco_nuevo->limite = segmento_encontrado->limite;

    t_memory_stick *ms_encontrado = buscar_ms_por_base(hueco_nuevo->base, NULL);
    hueco_nuevo->id = ms_encontrado->id;
    list_add(lista_huecos_memoria, hueco_nuevo);

    memoria_libre_actual += hueco_nuevo->limite;

    list_remove_element(proceso_encontrado->contexto.tabla_segmentos, segmento_encontrado);
    log_info(logger, "Segmento removido de la lista");
    
    free(segmento_encontrado);
    log_info(logger, "Segmento liberado");

    op_code respuesta = OK;

    send(socket_cliente, &respuesta, sizeof(op_code), 0);

    log_info(logger, "OK enviado correctamente");

    list_destroy_and_destroy_elements(paquete_respuesta, free);
}

void suspender(int socket_cliente)
{
    // Recibo el paquete
    t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i:", socket_cliente);
        exit(EXIT_FAILURE);
    }
    uint32_t pid = *(uint32_t *)list_get(paquete_respuesta, 0);

    // Busco el proceso para suspender todos sus segmentos

    t_proceso *proceso_encontrado = buscar_por_pid(lista_procesos, pid);
    t_list *tabla_segmentos = proceso_encontrado->contexto.tabla_segmentos;

    for (int i = 0; i < list_size(tabla_segmentos); i++)
    {

        t_segmento *segmento_actual = (t_segmento *)list_get(tabla_segmentos, i);

        // El (block_size - 1) hace que me rendonde hacia arriba
        int bloques_necesarios_por_segmento = (segmento_actual->limite + (block_size - 1)) / block_size;

        u_int32_t base_segmento_actualizada = segmento_actual->base;
        u_int32_t limite_segmento_actualizada = segmento_actual->limite;

        // Busco en que MS esta el el segmento

        t_memory_stick *ms_encontrado = buscar_ms_por_base(segmento_actual->id, NULL);

        for (int j = 1; j <= bloques_necesarios_por_segmento; j++)
        {
            void *buffer_temporal = calloc(1, block_size);
            int bloque_libre = buscar_bloque_libre();

            if (bloque_libre == -1)
            {
                log_error(logger, "No hay espacio disponible en SWAP");
                exit(EXIT_FAILURE);
            }

            t_paquete *paquete = crear_paquete(ESCRITURA_SWAP);
            agregar_a_paquete(paquete, &bloque_libre, sizeof(u_int32_t));

            // Le pido a MS la direccion fisica de lo que quiero pasar a disco

            void *contenido;
            u_int32_t direccion_fisica = ms_encontrado->base + base_segmento_actualizada;

            if (limite_segmento_actualizada < block_size)
            {

                contenido = solicitud_lectura_ms(direccion_fisica, limite_segmento_actualizada);
                memcpy(buffer_temporal, contenido, limite_segmento_actualizada);
            }
            else
            {
                // Le pido a MS la direccion fisica de lo que quiero pasar a disco
                contenido = solicitud_lectura_ms(direccion_fisica, block_size);
                memcpy(buffer_temporal, contenido, block_size);
            }
            // free(contenido);
            agregar_a_paquete(paquete, buffer_temporal, block_size);

            base_segmento_actualizada += block_size;
            limite_segmento_actualizada -= block_size;

            free(buffer_temporal);
            enviar_paquete(paquete, socket_swap);
            eliminar_paquete(paquete);

            int respuesta;
            recv(socket_swap, &respuesta, sizeof(respuesta), MSG_WAITALL);

            if (respuesta == OK)
            {
                log_info(logger, "Parte del segmento %u suspendido existosamente", segmento_actual->id);

                uint32_t *bloque_nuevo = malloc(sizeof(uint32_t));
                *bloque_nuevo = bloque_libre;

                list_add(proceso_encontrado->bloques_en_swap, bloque_nuevo);
                bitarray_set_bit(bitmap, bloque_libre);
            }
            else if (respuesta == ERROR)
            {
                log_info(logger, "Error en la suspensión del proceso %u", pid);
            }
            else
            {
                log_info(logger, "Respuesta recibida desconocida");
            }
        }

        // Actualizo los huecos libres del ms

        t_hueco *nuevo_hueco = malloc(sizeof(t_hueco));
        nuevo_hueco->base = segmento_actual->base;
        nuevo_hueco->id = ms_encontrado->id;
        nuevo_hueco->limite = segmento_actual->limite;
        list_add(lista_huecos_memoria, nuevo_hueco);

        log_info(logger, "Se suspendio el segmento %u completo con PID %u ", segmento_actual->id, pid);
    }

    log_info(logger, "Se suspendio el proceso con PID %u ", pid);
    list_destroy_and_destroy_elements(paquete_respuesta, free);
}

int buscar_bloque_libre()
{
    log_debug(logger, "Máxino del bitmpa: %zu", bitarray_get_max_bit(bitmap));
    for (int i = 0; i < bitarray_get_max_bit(bitmap); i++)
    {
        if (!bitarray_test_bit(bitmap, i))
        {
            return i;
        }
    }

    return -1;
}

void *solicitud_lectura_ms(u_int32_t direccion_fisica, u_int32_t tamano)
{
    t_paquete *paquete_ms = crear_paquete(SOLICITUD_LECTURA_MS);

    agregar_a_paquete(paquete_ms, &direccion_fisica, sizeof(uint32_t));
    agregar_a_paquete(paquete_ms, &tamano, sizeof(uint32_t));
    enviar_paquete(paquete_ms, socket_memory_stick);
    eliminar_paquete(paquete_ms);

    int cod_op = recibir_operacion(socket_memory_stick);

    if (cod_op != RESPUESTA_LECTURA_MS)
    {
        log_error(logger, "Error al recibir respuesta de lectura del Memory Stick");
        return NULL;
    }

    t_list *datos_leidos = recibir_paquete(socket_memory_stick);
    void *contenido = list_get(datos_leidos, 0);

    if (contenido == NULL)
    {
        log_error(logger, "No se recibieron datos del Memory Stick");
        list_destroy_and_destroy_elements(datos_leidos, free);
        return NULL;
    }

    list_destroy_and_destroy_elements(datos_leidos, free);
    return contenido;
}

//lee un bloque de SWAP
void *solicitud_lectura_swap(uint32_t numero_bloque)
{
    t_paquete *paquete = crear_paquete(LECTURA_SWAP);

    agregar_a_paquete(paquete, &numero_bloque, sizeof(uint32_t));

    enviar_paquete(paquete, socket_swap);
    eliminar_paquete(paquete);

    int cod_op = recibir_operacion(socket_swap);

    if (cod_op != RESPUESTA_LECTURA_SW)
    {
        log_error(logger, "Error al recibir respuesta de lectura del SWAP");
        return NULL;
    }

    t_list *datos = recibir_paquete(socket_swap);

    void *contenido = list_get(datos, 0);

    if (contenido == NULL)
    {
        list_destroy_and_destroy_elements(datos, free);
        return NULL;
    }

    list_destroy_and_destroy_elements(datos, free);

    return contenido;
}

// es la inversa de solicitud_lectura_ms
void solicitud_escritura_ms(uint32_t direccion_fisica, uint32_t tamano, void *contenido)
{
    t_paquete *paquete = crear_paquete(SOLICITUD_ESCRITURA_MS);

    agregar_a_paquete(paquete, &direccion_fisica, sizeof(uint32_t));
    agregar_a_paquete(paquete, &tamano, sizeof(uint32_t));
    agregar_a_paquete(paquete, contenido, tamano);

    enviar_paquete(paquete, socket_memory_stick);
    eliminar_paquete(paquete);

    int respuesta;

    recv(socket_memory_stick, &respuesta, sizeof(int), MSG_WAITALL);

    if (respuesta != OK)
    {
        log_error(logger, "Error al escribir en Memory Stick");
    }
}

void desuspender(int socket_cliente)
{
     t_list *paquete_respuesta = recibir_paquete(socket_cliente);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del cliente %i", socket_cliente);
        return;
    }

    uint32_t pid = *(uint32_t *)list_get(paquete_respuesta, 0);

    t_proceso *proceso = buscar_por_pid(lista_procesos, pid);

    if (proceso == NULL)
    {
        log_error(logger, "No existe el proceso %u", pid);
        list_destroy_and_destroy_elements(paquete_respuesta, free);
        return;
    }

    t_list *tabla_segmentos = proceso->contexto.tabla_segmentos;

    int indice_bloque_swap = 0;

    for (int i = 0; i < list_size(tabla_segmentos); i++)
    {
        t_segmento *segmento_actual = list_get(tabla_segmentos, i);

        int bloques_necesarios = (segmento_actual->limite + (block_size - 1)) / block_size;

        log_info(logger, "Desuspendiendo segmento %u (%d bloques)", segmento_actual->id, bloques_necesarios);


        //buscamos el hueco

        char *estrategia_de_asignacion = config_get_string_value(config, "ALLOCATION_STRATEGY");

        t_hueco *hueco_elegido = NULL;

        if (strcmp(estrategia_de_asignacion, "BEST") == 0)
        {
            for (int j = 0; j < list_size(lista_huecos_memoria); j++)
            {
                t_hueco *hueco_actual = list_get(lista_huecos_memoria, j);

                if (hueco_actual->limite >= segmento_actual->limite &&
                    (hueco_elegido == NULL ||
                    hueco_actual->limite < hueco_elegido->limite))
                {
                    hueco_elegido = hueco_actual;
                }
            }
        }
        else if (strcmp(estrategia_de_asignacion, "WORST") == 0)
        {
            for (int j = 0; j < list_size(lista_huecos_memoria); j++)
            {
                t_hueco *hueco_actual = list_get(lista_huecos_memoria, j);

                if (hueco_actual->limite >= segmento_actual->limite &&(hueco_elegido == NULL || hueco_actual->limite > hueco_elegido->limite))
                {
                    hueco_elegido = hueco_actual;
                }
            }
        }

        if(hueco_elegido == NULL)
        {
        log_error(logger, "No hay hueco para restaurar el segmento %u", segmento_actual->id);
        return;
        }

        //reservamos memoria

        segmento_actual->base = hueco_elegido->base;

        if (segmento_actual->limite == hueco_elegido->limite)
        {
            list_remove_element(lista_huecos_memoria, hueco_elegido);
            free(hueco_elegido);
        }
        else
        {
            hueco_elegido->base += segmento_actual->limite;
            hueco_elegido->limite -= segmento_actual->limite;
        }

        memoria_libre_actual -= segmento_actual->limite;

        uint32_t base_actual = segmento_actual->base;
        uint32_t limite_actual = segmento_actual->limite;

        //averiguamos que bloque de swap corresponde

        for(int j = 0; j < bloques_necesarios; j++)
        {
            uint32_t *bloque_swap = list_get(proceso->bloques_en_swap, indice_bloque_swap);

            if(bloque_swap == NULL)
            {
                log_error(logger, "No existe bloque swap para el indice %d", indice_bloque_swap);
                return;
            }

            void *contenido = solicitud_lectura_swap(*bloque_swap);

            if (contenido == NULL)
            {
                log_error(logger, "Error leyendo bloque %u de swap", *bloque_swap);
                return;
            }

            uint32_t bytes_a_escribir;

            if(limite_actual < block_size)
            {
                bytes_a_escribir = limite_actual;
            }
            else
            {
                bytes_a_escribir = block_size;
            }

            solicitud_escritura_ms (base_actual, bytes_a_escribir, contenido);

            base_actual += bytes_a_escribir;
            limite_actual -= bytes_a_escribir;

            bitarray_clean_bit(bitmap, *bloque_swap);

            free(contenido);

            indice_bloque_swap++;
        }
        
    }

    //restauramos los segmentos desde swap. los bloques ya no pertenecen al proceso

    list_destroy_and_destroy_elements(proceso->bloques_en_swap, free);
    proceso->bloques_en_swap = list_create();

    list_destroy_and_destroy_elements(paquete_respuesta, free);
}

