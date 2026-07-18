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
#include "registros.h" //incluimos el registros.h 

/**
* @brief Imprime un saludo por consola
* @param quien Módulo desde donde se llama a la función
* @return No devuelve nada
*/
void saludar(char* quien);

#define PUERTO "4444"
/*typedef enum
{
	MENSAJE,
	PAQUETE
}op_code; */

extern t_log* logger;

void* recibir_buffer(int*, int);

int iniciar_servidor(char*);
int esperar_cliente(int);
t_list* recibir_paquete(int);
void recibir_mensaje(int);
int recibir_operacion(int);

//funciones cliente:
typedef enum
{
	MENSAJE,
	PAQUETE
}op_code;

typedef struct
{
	int size;
	void* stream;
} t_buffer;

typedef struct
{
	op_code codigo_operacion;
	t_buffer* buffer;
} t_paquete;

typedef enum {
    CPU = 1,
    SWAP,
	IO,
    KERNEL_SCHEDULER,
    MEMORY_STICK
} tipo_modulo;

typedef enum { //Sirve para saber que es lo que el scheduler este haciendo en cada tarea
    ESTADO_NEW,        // Recien creado, esta en la "puerta" del sistema.
    ESTADO_READY,      // En la sala de espera, listo para usar la CPU.
    ESTADO_EXEC,       // Adentro de la oficina (CPU) trabajando.
    ESTADO_BLOCK,      // Durmiendo o esperando que termine una IO (pasa el tiempo).
    ESTADO_EXIT,       // Ya terminó todo y se está yendo del sistema.
    ESTADO_READY_SUSP, // Estaba listo pero lo mandamos al disco (SWAP) por falta de RAM.
    ESTADO_BLOCK_SUSP  // Estaba bloqueado y también lo mandamos al disco (SWAP)
} t_estado;

typedef struct {
    uint32_t pid;           // El DNI del proceso (0, 1, 2...)
    uint32_t prioridad;     // Que tan importante es (0 es la máxima)
    t_estado estado;        // usamos el enum que acabamos de crear (arriba)
    t_registros registros;  // registros.h
} t_pcb;


int crear_conexion(char* ip, char* puerto);
void enviar_mensaje(char* mensaje, int socket_cliente);
t_paquete* crear_paquete(void);
void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio);
void enviar_paquete(t_paquete* paquete, int socket_cliente);
void liberar_conexion(int socket_cliente);
void eliminar_paquete(t_paquete* paquete);


#endif /* UTILS_HELLO_H_ */
