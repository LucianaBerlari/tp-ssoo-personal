#ifndef UTILS_HELLO_H_
#define UTILS_HELLO_H_
#define _GNU_SOURCE // Lo puse para que me funciene esta libreria <netdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <commons/log.h>
#include <commons/config.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <commons/collections/list.h>
#include <string.h>
#include <assert.h>
#include <utils/protocolo.h>
#include <commons/bitarray.h>
#include <semaphore.h>
#include <pthread.h>

/**
 * @brief Imprime un saludo por consola
 * @param quien Módulo desde donde se llama a la función
 * @return No devuelve nada
 */
void saludar(char *quien);

#define PUERTO "4444"
/*typedef enum
{
	MENSAJE,
	PAQUETE
}op_code; */

extern t_log *logger;

// Comunicación con KM
	// Lo usamos para esperar la confirmación de KM 
extern sem_t sem_respuesta_memoria;
	// Lo usamos para que KM reciba un mensaje a la vez
extern pthread_mutex_t mx_comunicacion_memoria; 

// Estructuras
/*typedef enum
{
	MENSAJE,
	PAQUETE,
	
} op_code;*/

typedef struct
{
	int size;
	void *stream;
} t_buffer;

typedef struct
{
	op_code codigo_operacion;
	t_buffer *buffer;
} t_paquete;

typedef enum
{
	CPU = 1,
	SWAP,
	IO,
	KERNEL_SCHEDULER,
	MEMORY_STICK
} tipo_modulo;

/*typedef struct // NO se si es correcto ponerla aca. Pero la usa CPU Y KM
{
	uint32_t PC;
	uint32_t AX;
	uint32_t BX;
	uint32_t CX;
	uint32_t DX;
} t_contexto; */ //puse algo similar en contexto.h



// Funciones de conexión

int iniciar_servidor(char *);
int crear_conexion(char *ip, char *puerto);
int esperar_cliente(int);
void liberar_conexion(int socket_cliente);

// Funciones de paquetes

void crear_buffer(t_paquete* paquete);
t_paquete *crear_paquete(op_code codigo_opeacion);
void agregar_a_paquete(t_paquete *paquete, void *valor, int tamanio);
void* serializar_paquete(t_paquete* paquete, int bytes);
void enviar_paquete(t_paquete *paquete, int socket_cliente);
void eliminar_paquete(t_paquete *paquete);
void enviar_mensaje(char *mensaje, int socket_cliente);

void *recibir_buffer(int *, int);
t_list *recibir_paquete(int);
void recibir_mensaje(int);
int recibir_operacion(int);

char* recibir_nombre(int socket_cliente);

void enviar_creacion_proceso(int socket_memoria, uint32_t pid, char* path);

void enviar_finalizacion_proceso(int socket_memoria, uint32_t pid);



#endif /* UTILS_HELLO_H_ */
