#include <utils/utils.h>

t_log *logger;
void ejecutar_stdin(int socket_io);
void ejecutar_stdout(int socket_io);
void ejecutar_sleep_io(int socket_io);

int main(int argc, char *argv[])
{
	saludar("io");

	if (argc < 2)
	{
		printf("Uso correcto: ./bin/io [STDIN|STDOUT|SLEEP]\n");
		return 1;
	}

	char *s_tipo_io = argv[1];

	logger = log_create("io.log", "IO", 1, LOG_LEVEL_DEBUG);

	t_config *config = config_create("io.config");

	char *ip_kernel_scheduler = config_get_string_value(config, "IP_KERNEL_SCHEDULER");

	char *puerto_kernel_scheduler = config_get_string_value(config, "PUERTO_KERNEL_SCHEDULER");

	// Conexión
	 int socket_io = crear_conexion(ip_kernel_scheduler, puerto_kernel_scheduler);

	// PRUEBA (Lo comente porque la direccion 9001 no coincidia con la de ks)
	//int socket_io = crear_conexion("127.0.0.1", "9001");

	if (socket_io > 0)
	{
		log_info(logger, "## Conectado a Kernel Scheduler");

		int tipo = IO;
		send(socket_io, &tipo, sizeof(int), 0);
		log_info(logger, "Socket numero: %i", socket_io);
		//enviar_mensaje("Conexion OK con io", socket_io);
	}
	else
	{
		log_error(logger, "Error en la conexión");
		return 1;
	}

	int i_tipo_io;

		if (strcmp(s_tipo_io,"STDIN") == 0)
		{
			i_tipo_io = STDIN;
			send(socket_io, &i_tipo_io, sizeof(int), 0);
			
		}
		else if ( strcmp(s_tipo_io,"STDOUT") == 0)
		{
			i_tipo_io = STDOUT;
			send(socket_io, &i_tipo_io, sizeof(int), 0);
		}
		else if (strcmp(s_tipo_io,"SLEEP") == 0)
		{
			i_tipo_io = SLEEP;
			send(socket_io, &i_tipo_io, sizeof(int), 0);
		}else{
			log_error(logger, "No es un tipo de IO válida");
		}

		log_info(logger, "Tipo de interfaz: %s / %i", s_tipo_io, i_tipo_io);
	

	while (1) 
	{
		int operacion_recibida =0; 
		operacion_recibida = recibir_operacion(socket_io);
		log_info(logger,"Recibi la operacion: %i\n", operacion_recibida);
		
		if(operacion_recibida < 0){
			log_info(logger,"Se desconectó el servidor");
			break;
		}

		if(operacion_recibida != i_tipo_io)
		{
			log_info(logger, "No coincide la operacion %i con el tipo de io %i. Verificar haber inicializo la io", operacion_recibida, i_tipo_io);
			continue;
		}

		switch (operacion_recibida)
		{
		case STDIN://no usamos S_STDIN porque verificamos que sea igual al TIPO de IO ¿?
			ejecutar_stdin(socket_io);
			break;
		case STDOUT://no usamos S_
			ejecutar_stdout(socket_io);
			break;
		case SLEEP://no usamos S_
			ejecutar_sleep_io(socket_io);
			break;
		default:
			break;
		}
		// sleep(1);
	}

	config_destroy(config);
	log_destroy(logger);
	return 0;
}

void ejecutar_stdin(int socket_io)
{
	
	// Recibo pid y tamaño de ks
	t_list *paquete_recibido = recibir_paquete(socket_io);

	if(paquete_recibido == NULL){
		log_error(logger,"No se recibio el paquete correctamente");
		exit(EXIT_FAILURE);
	}

	if(list_size(paquete_recibido) == 0){
		log_error(logger,"No hay elementos");
		exit(EXIT_FAILURE);
	}

	uint32_t pid = *(uint32_t *)list_get(paquete_recibido, 0);

	uint32_t tamanio = *(uint32_t *)list_get(paquete_recibido, 1);

	if(tamanio == 0){
		log_error(logger, "PID: %u -  Se solicito leer 0 caracteres", pid);
		return;
	}

	log_info("PID: %u. Tamaño %u", pid, tamanio);

	log_info(logger, "## PID: %u - Inicio de IO", pid);

	// Recibo por terminal el input del usuario
	log_info(logger, "## PID: %u - Ingrese %u caracteres:", pid, tamanio);

	t_buffer buffer;
	buffer.stream = malloc(tamanio);
	buffer.size = tamanio+1;

	char *input = fgets(buffer.stream, buffer.size, stdin);

	if (input == NULL)
	{
		log_error(logger, "PID: %u - No se pudo leer la entrada del usuario", pid);
		list_destroy_and_destroy_elements(paquete_recibido, free);
		free(buffer.stream);
		exit(EXIT_FAILURE);
	}

	size_t tamanio_input = strcspn(input, "\n"); // sin  \n

	// Chequeo el tamaño con lo pedido
	if (buffer.size > strlen(input))
	{

		for (int i = tamanio_input; i < tamanio; i++)
		{
			input[i] = '\0';
		}
	}

	// Mando la información a KS
	log_info(logger,"Enviando info al ks");
	t_paquete *paquete = crear_paquete(STDIN_RESPUESTA);
	agregar_a_paquete(paquete, &pid, sizeof(u_int32_t));
	agregar_a_paquete(paquete, buffer.stream, buffer.size);
	enviar_paquete(paquete, socket_io);
	eliminar_paquete(paquete);
	list_destroy_and_destroy_elements(paquete_recibido, free);
	free(buffer.stream);

	log_info(logger, "## PID: %u - Fin de IO”", pid);
}

void ejecutar_stdout(int socket_io)
{

	// Recibo pid y tamaño de ks
	t_list *paquete_recibido = recibir_paquete(socket_io);
	uint32_t pid = *(uint32_t *)list_get(paquete_recibido, 0);
	uint32_t tamanio = *(uint32_t *)list_get(paquete_recibido, 1);
	char *cadena = (char *)list_get(paquete_recibido, 2);

	log_info(logger, "## PID: %u - Inicio de IO”", pid);

	log_info(logger, "## PID: %u - %.*s", pid, tamanio, cadena);

	t_paquete *paquete = crear_paquete(STDOUT_RESPUESTA);
	agregar_a_paquete(paquete, &pid, sizeof(uint32_t));
	enviar_paquete(paquete, socket_io);

	eliminar_paquete(paquete);
	list_destroy_and_destroy_elements(paquete_recibido, free);

	log_info(logger, "## PID: %u - Fin de IO”", pid);
}

void ejecutar_sleep_io(int socket_io)
{
	// Recibo pid y tamaño de ks
	t_list *paquete_recibido = recibir_paquete(socket_io);
	uint32_t pid = *(uint32_t *)list_get(paquete_recibido, 0);
	uint32_t tiempo = *(uint32_t *)list_get(paquete_recibido, 1);

	log_info(logger, "## PID: %u - Inicio de IO”", pid);
	log_info(logger, "## PID: %u - Haciendo sleep por %u milisegundos.", pid, tiempo);
	usleep(tiempo*1000);

	t_paquete *paquete = crear_paquete(SLEEP_RESPUESTA);
	agregar_a_paquete(paquete, &pid, sizeof(uint32_t));
	enviar_paquete(paquete, socket_io);

	eliminar_paquete(paquete);
	list_destroy_and_destroy_elements(paquete_recibido, free);

	log_info(logger, "## PID: %u - Fin de IO”", pid);
}
