#include <utils/utils.h>

sem_t sem_respuesta_memoria;
pthread_mutex_t mx_comunicacion_memoria;

void saludar(char *quien)
{
	printf("Hola desde %s!!\n", quien);
}

// t_log* logger; ya tenemos un extern
// era: int iniciar_servidor (). cambiamos a char

// Funciones de conexiones
int iniciar_servidor(char *puerto)
{
	// Quitar esta línea cuando hayamos terminado de implementar la funcion
	// assert(!"no implementado!"); lo quitamos

	int socket_servidor;

	struct addrinfo hints, *servinfo /*, *p*/;
	// el  *p hay que usarlo para la lista enlazada, comento solo  para probar conexion

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(NULL, puerto, &hints, &servinfo); // nos guarda en serverino un puntero que apunta hacia los datos necesarios para la creación del socket...

	//  Creamos el socket de escucha del servidor
	socket_servidor = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);

	// Permite reutilizar el puerto inmediatamente después de terminar la ejecución
	int activador = 1;
	if (activador)
	{
		setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &activador, sizeof(activador));
	}

	// Asociamos el socket a un puerto
	bind(socket_servidor, servinfo->ai_addr, servinfo->ai_addrlen);
	// Escuchamos las conexiones entrantes
	listen(socket_servidor, SOMAXCONN); // listen(FD_ESCUCHA, PARÁMETRO DEL KERNEL DE LINUX establece el núm
	// max de conexiones que puedn estar en cola para un socket);

	freeaddrinfo(servinfo);
	log_trace(logger, "Listo para escuchar a mi cliente");

	return socket_servidor;
}

int crear_conexion(char *ip, char *puerto)
{
	struct addrinfo hints;
	struct addrinfo *server_info;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, puerto, &hints, &server_info);

	// Ahora vamos a crear el socket.
	int socket_cliente = socket(server_info->ai_family,
								server_info->ai_socktype,
								server_info->ai_protocol);

	// Ahora que tenemos el socket, vamos a conectarlo //es decir asociamos el socket a un puerto
	if (connect(socket_cliente, server_info->ai_addr, server_info->ai_addrlen) == -1)
	{
		// SI falla liberamos memoria y retornamos -1
		freeaddrinfo(server_info);
		close(socket_cliente); // Cerramos el socket que no se pudo usar
		return -1;
	}

	freeaddrinfo(server_info); // liberamos la memoria?

	return socket_cliente; // Si no la funcion connect no retorna -1, devuelve un numero de socket.
}

int esperar_cliente(int socket_servidor)
{
	// Quitar esta línea cuando hayamos terminado de implementar la funcion
	// assert(!"no implementado!");

	// Aceptamos un nuevo cliente
	// int socket_cliente= accept(socket_servidor, NULL,NULL); //no va?
	// int fd_conexion=accept(fd_escucha,NULL,NULL);//los NULL son informacion como ip o puerto?
	int socket_cliente = accept(socket_servidor, NULL, NULL);
	log_info(logger, "Se conecto un cliente!");

	return socket_cliente;
}

void liberar_conexion(int socket_cliente)
{
	close(socket_cliente);
}

// Funciones de paquetes

void crear_buffer(t_paquete *paquete)
{
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = 0;
	paquete->buffer->stream = NULL;
}

t_paquete *crear_paquete(op_code codigo_operacion)
{
	t_paquete *paquete = malloc(sizeof(t_paquete));
	paquete->codigo_operacion = codigo_operacion;
	crear_buffer(paquete);
	return paquete;
}

void agregar_a_paquete(t_paquete *paquete, void *valor, int tamanio)
{
	paquete->buffer->stream = realloc(paquete->buffer->stream, paquete->buffer->size + tamanio + sizeof(int));

	memcpy(paquete->buffer->stream + paquete->buffer->size, &tamanio, sizeof(int));
	memcpy(paquete->buffer->stream + paquete->buffer->size + sizeof(int), valor, tamanio);

	paquete->buffer->size += tamanio + sizeof(int);
}

void *serializar_paquete(t_paquete *paquete, int bytes)
{
	void *magic = malloc(bytes);
	int desplazamiento = 0;

	memcpy(magic + desplazamiento, &(paquete->codigo_operacion), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(magic + desplazamiento, &(paquete->buffer->size), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(magic + desplazamiento, paquete->buffer->stream, paquete->buffer->size);
	desplazamiento += paquete->buffer->size;

	return magic;
}

void enviar_paquete(t_paquete *paquete, int socket_cliente)
{
	int bytes = paquete->buffer->size + 2 * sizeof(int);
	void *a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
}

void eliminar_paquete(t_paquete *paquete)
{
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}

void enviar_mensaje(char *mensaje, int socket_cliente)
{
	t_paquete *paquete = malloc(sizeof(t_paquete));

	paquete->codigo_operacion = MENSAJE;
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = strlen(mensaje) + 1;
	paquete->buffer->stream = malloc(paquete->buffer->size);
	memcpy(paquete->buffer->stream, mensaje, paquete->buffer->size);

	int bytes = paquete->buffer->size + 2 * sizeof(int);

	void *a_enviar = serializar_paquete(paquete, bytes);

	send(socket_cliente, a_enviar, bytes, 0);

	free(a_enviar);
	eliminar_paquete(paquete);
}

void *recibir_buffer(int *size, int socket_cliente)
{
	void *buffer;

	recv(socket_cliente, size, sizeof(int), MSG_WAITALL);
	buffer = malloc(*size);
	recv(socket_cliente, buffer, *size, MSG_WAITALL);

	return buffer;
}

t_list *recibir_paquete(int socket_cliente)
{
	int size;
	int desplazamiento = 0;
	void *buffer;
	t_list *valores = list_create();
	int tamanio;

	buffer = recibir_buffer(&size, socket_cliente);
	while (desplazamiento < size)
	{
		memcpy(&tamanio, buffer + desplazamiento, sizeof(int));
		desplazamiento += sizeof(int);
		char *valor = malloc(tamanio);
		memcpy(valor, buffer + desplazamiento, tamanio);
		desplazamiento += tamanio;
		list_add(valores, valor);
	}
	free(buffer);

	return valores;
}

void recibir_mensaje(int socket_cliente)
{
	int cod_op = recibir_operacion(socket_cliente);

	if (cod_op != MENSAJE)
	{
		log_error(logger, "No se esta recibiendo la operación MENASAJE");
		log_info(logger, "El cod op recibido fue: %i", cod_op);
		return;
	}

	int size;
	char *buffer = recibir_buffer(&size, socket_cliente);
	log_info(logger, "Me llego el mensaje: %s", buffer);

	free(buffer);
}

int recibir_operacion(int socket_cliente)
{
	int cod_op;
	if (recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL) > 0) // el recv es bloqueante
		return cod_op;
	else
	{
		close(socket_cliente);
		return -1;
	}
}

char *recibir_nombre(int socket_cliente)
{
	int tamanio;
	if (recv(socket_cliente, &tamanio, sizeof(int), MSG_WAITALL) <= 0)
	{
		return NULL;
	}
	char *nombre = malloc(tamanio);
	recv(socket_cliente, nombre, tamanio, MSG_WAITALL);
	return nombre;
}

void enviar_creacion_proceso(int socket_memoria, uint32_t pid, char *path)
{
	pthread_mutex_lock(&mx_comunicacion_memoria);
	t_paquete *paquete = crear_paquete(CARGA_PROCESO);

	// Calculamos el tamaño del path (incluyendo el \0)
	int path_len = strlen(path) + 1;

	// Agregamos el PID al buffer
	agregar_a_paquete(paquete, &pid, sizeof(uint32_t));

	// Agregamos el tamaño del string y luego el string
	// (Esto es clave para que la memoria sepa cuánto leer)
	agregar_a_paquete(paquete, &path_len, sizeof(int));
	agregar_a_paquete(paquete, path, path_len);

	enviar_paquete(paquete, socket_memoria);
	eliminar_paquete(paquete);
	pthread_mutex_unlock(&mx_comunicacion_memoria);
	sem_wait(&sem_respuesta_memoria);
	log_info(logger, "Enviada solicitud de creación de proceso PID %d a Memoria\n", pid);
	
}

void enviar_finalizacion_proceso(int socket_memoria, uint32_t pid)
{ 
	pthread_mutex_lock(&mx_comunicacion_memoria);
	// LE avisamos a la memoria que termino un proceso para que libere sus estructuras
	t_paquete *paquete = crear_paquete(FINALIZAR_PROCESO);

	agregar_a_paquete(paquete, &pid, sizeof(uint32_t));

	enviar_paquete(paquete, socket_memoria);
	eliminar_paquete(paquete);
	pthread_mutex_unlock(&mx_comunicacion_memoria);
	sem_wait(&sem_respuesta_memoria);
	log_info(logger, "Respuesta OK sobre la eliminación del proceso con PID %u procesada", pid);
	
	
	/*int respuesta;
	recv(socket_memoria, &respuesta, sizeof(respuesta), MSG_WAITALL); // Lo agregue para que haya un orden fijo --> 1.crear proceso 2.actualizar contexto 3.eliminar proceso
	if (respuesta == OK)
	{
		log_info(logger, "Respuesta OK sobre la eliminación del proceso con PID %u", pid);
	}
	else
	{
		log_info(logger, "ERROR en la respuesta de la eliminación del proceso con PID%u", pid);
	}*/
}