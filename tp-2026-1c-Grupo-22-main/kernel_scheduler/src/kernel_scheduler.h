#ifndef SERVER_H_
#define SERVER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <commons/log.h>
#include <commons/collections/list.h>
#include <utils/utils.h>
#include <utils/contexto.h>
#include <semaphore.h>
#include <pthread.h>
#define MAX_PRIORIDADES 32

// ESTADOS DEL PROCESOS QUE REPRESENTAN LOS C.V.enum
typedef enum
{
    NEW,
    READY,
    EXEC,
    BLOCK,
    EXIT
} t_estado;
typedef struct
{
    uint32_t pid;       // Identificador unico del procesos
    uint32_t prioridad; // Prioridad para planificacion
    uint32_t prioridad_original;
    t_estado estado;        // EStado actual(NEW,READY,ETC...)
    t_contexto *contexto;   // Registros de CPU guardados
    uint32_t dir_logica_io; // para guardar la dir cuando hay IO pendiente
    uint32_t tamanio_io;
} t_pcb;

// ESTRUCTURA DE CPU CONECTADA, AHORA NECESITO SABER QUE CPUS ESTAN
// DISPONIBLES PARA MANDAR PROCESOS

typedef enum
{
    FIFO,
    RR,
    CMN
} t_algoritmo;

void *atender_cliente(void *arg);
t_pcb *crear_pcb(uint32_t pid, uint32_t prioridad);
void *planificador_largo_plazo(void *arg);
void iteratorr(char *value);
void *planificador_corto_plazo(void *arg); // El que manda a CPUs
void *hilo_quantum(void *arg);             // para Round Robin
#endif                                     /* SERVER_H_ */

void *escuchar_kernel_memory(void *arg);

// INFORMACION QUE DEJAMOS EN CADA MUTEX
typedef struct
{
    char *nombre;
    uint32_t pid_dueno;                // PID del que lo tiene. -1 si esta libre
    uint32_t prioridad_original_dueno; // Para devolverle su fuerza real despues
    t_list *cola_bloqueados;           // Lista de procesos esperando este mutex
    pthread_mutex_t mx_mutex;          // Candado de C para proteger esta estructura
} t_mutex_kernel;

// Lista global donde guardaremos todos los mutex que existan
t_list *lista_mutex_globales;
pthread_mutex_t mx_lista_mutex;

// Prototipo de funciones

t_pcb *buscar_pcb_en_ejecucion(int socket_fd);
t_pcb *buscar_pcb_por_pid(uint32_t pid);
t_mutex_kernel *buscar_mutex(char *nombre);
void ejecutar_mutex_create(char *nombre_mutex);
void ejecutar_mutex_lock(t_pcb *pcb_solicitante, char *nombre_mutex);
void ejecutar_mutex_unlock(t_pcb *pcb_dueno, char *nombre_mutex);

typedef struct
{
    int socket_dispatch;  // Puerto 8002
    int socket_interrupt; // Puerto 8003
    int cpu_id;           // Identificador unico de la CPU (Ej: 22)
    int ocupada;
    t_pcb *pcb_ejecutando;

    pthread_t hilo_quantum_activo;
    int quantum_vigente;
} t_cpu_info;

extern t_list *colas_ready[MAX_PRIORIDADES];
extern int cantidad_colas_configuradas; // Se lee del config
extern bool preemption_habilitada;      // QUEUE_PREEMPTION (true/false)