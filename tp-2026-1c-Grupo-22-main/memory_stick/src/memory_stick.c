#include <pthread.h>
#include <utils/utils.h>

// Variables de configuración
t_log_level log_level;
t_config *config;
t_log *logger;
int socket_servidor;
char *puerto_escucha;
char *ip_memory_stick;
char *ip_kernel_memory;
char *puerto_kernel_memory;
u_int32_t tamano_memoria_i;
char *tamano_memoria_s;
char *memoria_principal;
int memory_delay;

// Funciones de configuracion
void ObtenerConfig(void);
void iterator(char *value);
void *atender_cliente(void *arg);
void *atender_kernel_memory(void *arg);
void *atender_peticiones(int socket_kernel_memory, int cod_op);

// Funciones de las operaciones
void lectura(int socket);
void escritura(int socket);

int main(int argc, char *argv[])
{
    saludar("memory_stick");

    if (argc < 3)
    {
        printf("Uso correcto: ./bin/memory_stick [Archivo Config] [Tamaño] \n");
        exit(EXIT_FAILURE);
    }

    config = config_create(argv[1]);

    if (config == NULL)
    {
        printf("No se pudo cargar el config en la ruta: %s\n", argv[1]);
        exit(EXIT_FAILURE);
    }

    tamano_memoria_s = argv[2];
    ObtenerConfig();
    memoria_principal = malloc(tamano_memoria_i); // Pido memoria para que vaya a heap, y no a stack (es muy grande para stack)
    memset(memoria_principal,0,tamano_memoria_i);
    // Parte del cliente
    int socket_memory_stick = crear_conexion(ip_kernel_memory, puerto_kernel_memory);

    if (socket_memory_stick > 0)
    {
        log_info(logger, "## Conectado a Kernel Memory");
        int tipo = MEMORY_STICK;
        send(socket_memory_stick, &tipo, sizeof(int), 0);
        send(socket_memory_stick, &tamano_memoria_i, sizeof(uint32_t), 0);
        send(socket_memory_stick, puerto_escucha, 20, 0); //probamos enviando puerto

        pthread_t hilo_kernel_memory;
        int *socket_km_ptr = malloc(sizeof(int));
        *socket_km_ptr = socket_memory_stick;

        pthread_create(&hilo_kernel_memory, NULL, atender_kernel_memory, socket_km_ptr);
        pthread_detach(hilo_kernel_memory);
    }
    else
    {
        log_error(logger, "Error en la conexión con Kernel Memory");
    }

    // Parte del servidor

    int socket_servidor = iniciar_servidor(puerto_escucha);

    if (socket_servidor < 0)
    {
        log_error(logger, "Error al iniciar servidor");
        config_destroy(config);
        exit(EXIT_FAILURE);
    }

    log_info(logger, "Servidor listo para recibir clientes");

    while (1)
    {
        int socket_cliente = esperar_cliente(socket_servidor);

        if (socket_cliente < 0)
        {
            log_error(logger, "Error al aceptar cliente");
            continue;
        }

        log_info(logger, "Se conectó un cliente");

        pthread_t hilo;
        int *socket_ptr = malloc(sizeof(int));
        *socket_ptr = socket_cliente;

        pthread_create(&hilo, NULL, atender_cliente, socket_ptr);
        pthread_detach(hilo);
    }

    log_destroy(logger);
    config_destroy(config);

    return EXIT_SUCCESS;
}

void iterator(char *value)
{
    log_info(logger, "%s", value);
}

void ObtenerConfig(void)
{

    tamano_memoria_i = strtoul(tamano_memoria_s, NULL, 10);
    ip_memory_stick = config_get_string_value(config, "IP_MEMORYSTICK");
    puerto_escucha = config_get_string_value(config, "PUERTO_MEMORYSTICK");
    log_level = log_level_from_string(config_get_string_value(config, "LOG_LEVEL"));
    logger = log_create("log.log", "Servidor", 1, log_level);
    ip_kernel_memory = config_get_string_value(config, "IP_KERNELMEMORY");
    puerto_kernel_memory = config_get_string_value(config, "PUERTO_KERNEL");
    memory_delay = config_get_int_value(config, "MEMORY_DELAY");
}

void *atender_kernel_memory(void *arg)
{

    int socket_kernel_memory = *(int *)arg;
    free(arg);

    while (1)
    {
        int cod_op = recibir_operacion(socket_kernel_memory);
        log_info(logger, "Codigo operacion recibido: %d", cod_op);

        if (cod_op == -1)
        {
            log_warning(logger, "Kernel Memory desconectado. Socket: %i", socket_kernel_memory);
            close(socket_kernel_memory);
            exit(EXIT_FAILURE);
        }

        atender_peticiones(socket_kernel_memory, cod_op);
    }
}

void *atender_cliente(void *arg)
{
    int socket_cpu = *(int *)arg;
    free(arg);

    int tipo;
    recv(socket_cpu, &tipo, sizeof(int), MSG_WAITALL);

    if (tipo == CPU)
    {
        int id_cpu;
        recv(socket_cpu, &id_cpu, sizeof(int), 0);
        log_info(logger, "CPU ID: %d", id_cpu);
    }
    else
    {
        log_warning(logger, "Modulo desconocido");
        close(socket_cpu);
    }

    while (1)
    {
        int cod_op = recibir_operacion(socket_cpu);
        log_info(logger, "Codigo operacion recibido: %d", cod_op);

        if (cod_op == -1)
        {
            log_warning(logger, "Cliente desconectado");
            close(socket_cpu);
            return NULL;
        }

        atender_peticiones(socket_cpu, cod_op);
    }
}

void *atender_peticiones(int socket, int cod_op)
{
    log_info(logger, "Codigo operacion recibido: %d", cod_op);

    if (cod_op == -1)
    {
        log_warning(logger, "Cliente desconectado");
        close(socket);
        return NULL;
    }

    switch (cod_op)
    {
    case MENSAJE:
    {
        recibir_mensaje(socket);
        break;
    }
    case PAQUETE:
    {
        t_list *lista = recibir_paquete(socket);
        list_iterate(lista, (void *)iterator);

        break;
    }
    case SOLICITUD_LECTURA_MS:
    {
        lectura(socket);
        break;
    }
    case SOLICITUD_ESCRITURA_MS:
    {
        escritura(socket);
        break;
    }
    default:
        log_warning(logger, "Operacion desconocida");
        close(socket);
        return NULL;
        break;
    }
    return NULL;
}

void lectura(int socket)
{

    /*Lectura de datos
    El módulo Memory Stick recibirá una dirección física y un tamaño a leer. Como resultado de esta operación deberá devolver los bytes leídos.
    */

    t_list *paquete_recibido = recibir_paquete(socket);

    if (paquete_recibido == NULL)
    {
        log_error(logger, "Error al recibir el paquete del socket %i:", socket);
        exit(EXIT_FAILURE);
    }

    u_int32_t direccion_fisica = *(uint32_t *)list_get(paquete_recibido, 0);
    u_int32_t tamano_recibido = *(uint32_t *)list_get(paquete_recibido, 1);

    log_info(logger, "## Lectura de %u bytes", tamano_recibido);

    char *respuesta = malloc(tamano_recibido + 1);
    respuesta[tamano_recibido] = '\0';

    /*for (int i = 0; i < tamano_recibido; i++)
    {
      respuesta[i]=memoria_principal[direccion_fisica+i];
    }*/
    usleep(memory_delay * 1000);
    memcpy(respuesta, &memoria_principal[direccion_fisica], tamano_recibido);

    log_info(logger, "Palabra: %s", respuesta);

    t_paquete *paquete_respuesta = crear_paquete(RESPUESTA_LECTURA_MS);
    agregar_a_paquete(paquete_respuesta, respuesta, tamano_recibido);
    enviar_paquete(paquete_respuesta, socket);

    eliminar_paquete(paquete_respuesta);
    free(respuesta);
    list_destroy_and_destroy_elements(paquete_recibido, free);
}

void escritura(int socket)
{
    /*Escritura de datos
    Para esta operación se recibirá la dirección física a partir de la cual se deberá escribir y el contenido a escribir.
    Los grupos podrán añadir información extra para facilitar la implementación de esta operación.
    Como resultado de esta operación deberán devolver una confirmación al módulo que los haya llamado.
    */

    t_list *paquete_recibido = recibir_paquete(socket);

    if (paquete_recibido == NULL)
    {
        log_error(logger, "Error al recibir el paquete del socket %i:", socket);
        exit(EXIT_FAILURE);
    }

    u_int32_t direccion_fisica = *(uint32_t *)list_get(paquete_recibido, 0);
    u_int32_t tamano = *(uint32_t *)list_get(paquete_recibido, 1);
    //void *contenido = list_get(paquete_recibido, 2);//pongo el char * solo porque tiraba error el make
    char *contenido = (char *)list_get(paquete_recibido, 2);

    usleep(memory_delay * 1000);

    /* Posible error ya que funciona solo con STring y no con uint32_t
    
    int tamano_contenido = 0;
    while (contenido[tamano_contenido] != '\0')
    {
        tamano_contenido++;
    }*/
    memcpy(&memoria_principal[direccion_fisica], contenido, tamano);
    log_info(logger, "## Escritura de %u bytes", tamano);
    log_info(logger, "Escritura en BASE = %u TAMAÑO = %u CONTENIDO: %s", direccion_fisica, tamano, contenido);

    int respuesta = OK;
    send(socket, &respuesta, sizeof(respuesta), MSG_WAITALL);

    list_destroy_and_destroy_elements(paquete_recibido, free);
}