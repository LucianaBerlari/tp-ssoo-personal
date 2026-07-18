#include <utils/utils.h>

t_log *logger;
t_log_level log_level;
t_config *config;
void obtenerConfig();
char *ip_kernel_memory;
char *puerto_kernel_memory;
int socket_kernel_memory;
int conectar(char *ip, char *puerto);

char *file_path;
size_t file_size;
size_t block_size;

FILE *archivo_swap;

void escritura(int socket_kernel_memory);
void lectura(int socket_kernel_memory);

int main(int argc, char *argv[])
{
    // ===== CONFIGURACIÓN ======
    saludar("Swap");

    if (argc == 1)
    {
        printf("Uso correcto: ./bin/swap [Archivo Config] \n");
        exit(EXIT_FAILURE);
    }

    config = config_create(argv[1]);

    if (config == NULL)
    {
        printf("No se pudo cargar el config en la ruta: %s\n", argv[1]);
        exit(EXIT_FAILURE);
    }

    obtenerConfig();

    // ===== CONEXIÓN ======
    log_info(logger, "Intentando conectar a Memoria IP: %s y PUERTO: %s", ip_kernel_memory, puerto_kernel_memory);

    socket_kernel_memory = conectar(ip_kernel_memory, puerto_kernel_memory);
    log_info(logger, "## Conectado a Kernel Memory");

    int tipo = SWAP;
    send(socket_kernel_memory, &tipo, sizeof(int), 0);
    send(socket_kernel_memory, &file_size, sizeof(int), 0);
    log_info(logger,"Tamaño SWAP: %zu", file_size);
    send(socket_kernel_memory, &block_size, sizeof(int), 0);
    log_info(logger,"Tamaño bloque lógico: %zu", block_size);

    log_info(logger, "Intentando abrir la ruta: [%s]", file_path);

    archivo_swap = fopen("SWAP", "w+r");

    if (archivo_swap == NULL)
    {
        log_error(logger, "No se pudo abrir/crear el archivo de SWAP");
    }

    log_info(logger, "Esperando ordenes de Kernel Memory!");

    while (1)
    {
        int cod_op = recibir_operacion(socket_kernel_memory);

        if (cod_op == -1)
        {
            log_error(logger, "Sin conexión con Kernel Memory");
            break;
        }

        switch (cod_op)
        {
        case ESCRITURA_SWAP:
            escritura(socket_kernel_memory);
            break;
        case LECTURA_SWAP:
            lectura(socket_kernel_memory);
            break;
        default:
            log_warning(logger, "Operación desconocida");
            break;
        }
    }

    // ======= LIMPIEZA ======
    config_destroy(config);
    if (socket_kernel_memory != -1)
    {
        liberar_conexion(socket_kernel_memory);
    }
    log_info(logger, "SWAP desconectado");
    log_destroy(logger);

    return 0;
}

void obtenerConfig()
{
    ip_kernel_memory = config_get_string_value(config, "IP_MEMORIA");
    puerto_kernel_memory = config_get_string_value(config, "PUERTO_MEMORIA");
    log_level = log_level_from_string(config_get_string_value(config, "LOG_LEVEL"));
    logger = log_create("swap.log", "SWAP", 1, log_level);

    file_path = config_get_string_value(config, "SWAP_FILE_PATH");
    char *file_size_c = config_get_string_value(config, "SWAP_FILE_SIZE");
    file_size = strtoul(file_size_c, NULL, 10);
    char *block_size_c = config_get_string_value(config, "BLOCK_SIZE");
    block_size = strtoul(block_size_c, NULL, 10);
}

int conectar(char *ip, char *puerto)
{
    int socket_resultado = crear_conexion(ip, puerto);

    if (socket_resultado <= 0)
    {
        log_info(logger, "Error. Conectado a %s:%s\n", ip, puerto);
        return -1;
    }

    printf("Conectado exitosamente a %s:%s\n", ip, puerto);
    return socket_resultado;
}

void escritura(int socket_kernel_memory)
{

    t_list *paquete_respuesta = recibir_paquete(socket_kernel_memory);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta de Kernel Memory %i:", socket_kernel_memory);
        exit(EXIT_FAILURE);
    }
    uint32_t numero_bloque = *(uint32_t *)list_get(paquete_respuesta, 0);
    void *contenido = list_get(paquete_respuesta, 1);

    fseek(archivo_swap, block_size * numero_bloque, SEEK_SET);
    size_t bytes_escritos = fwrite(contenido, 1, block_size, archivo_swap);

    int respuesta;

    if (bytes_escritos == block_size)
    {
        log_info(logger, "Operación de escritura exitosa");
        respuesta = SUSPENSION_OK;
    }
    else
    {
        log_error(logger, "Operación de escritura erronea");
        respuesta = SUSPENSION_ERROR;
    }

    send(socket_kernel_memory, &respuesta, sizeof(respuesta), 0);
    list_destroy_and_destroy_elements(paquete_respuesta, free);
};

void lectura(int socket_kernel_memory)
{
    t_list *paquete_respuesta = recibir_paquete(socket_kernel_memory);

    if (paquete_respuesta == NULL)
    {
        log_error(logger, "Error al recibir la respuesta del Kernel Memory %i:", socket_kernel_memory);
        exit(EXIT_FAILURE);
    }
    uint32_t numero_bloque = *(uint32_t *)list_get(paquete_respuesta, 0);

    void *contenido = malloc(block_size);

    fseek(archivo_swap, block_size * numero_bloque, SEEK_SET);

    size_t bytes_leidos = fread(contenido, 1, block_size, archivo_swap);

    if (bytes_leidos != block_size)
    {
        log_error(logger, "Operación de lectura erronea");
        list_destroy_and_destroy_elements(paquete_respuesta, free);
        free(contenido);
        return;
    }

    log_info(logger, "Operación de lectura exitosa");

    t_paquete *paquete = crear_paquete(RESPUESTA_LECTURA_SW);
    agregar_a_paquete(paquete, contenido, block_size);
    enviar_paquete(paquete, socket_kernel_memory);
    eliminar_paquete(paquete);
    list_destroy_and_destroy_elements(paquete_respuesta, free);
    free(contenido);
}