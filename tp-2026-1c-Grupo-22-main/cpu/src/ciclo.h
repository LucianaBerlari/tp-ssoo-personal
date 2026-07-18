/*#ifndef CICLO_CPU_H
#define CICLO_CPU_H

#include <utils/contexto.h>
#include <utils/protocolo.h>
#include <commons/collections/list.h>
#include <utils/utils.h>

void ejecutar_proceso(int pid, t_contexto* ctx, int socket_memoria);
char* fetch_instruccion(int socket_memoria, int pid, int pc);

typedef struct {
    int op; 
    int pid;
    int pc;
} t_fetch;
#endif */

#ifndef CICLO_CPU_H
#define CICLO_CPU_H

#include <utils/contexto.h>
#include <utils/protocolo.h>
#include <commons/collections/list.h>
#include <utils/utils.h>

//para interrupciones ¡?
extern volatile bool hay_interrupcion;
extern int pid_interrupcion;
extern pthread_mutex_t mutex_interrupcion;
extern volatile bool hay_interrupcion;//solo para probar interrupción hardcodeada
// --- Opcode ---
/*typedef enum {
    NOOP, SET, MOV_IN, MOV_OUT, SUM, SUB, JNZ, COPY_MEM,
    MUTEX_CREATE, MUTEX_LOCK, MUTEX_UNLOCK,
    MEM_ALLOC, MEM_FREE, SLEEP,
    STDOUT_OP, STDIN_OP, INIT_PROC, EXIT_PROC
} t_opcode;*/

// --- Instrucción ---
typedef struct {
    op_code tipo;
    char* args[3];
    int cant_args;
} t_instruccion;

// --- Resultado del execute ---
typedef enum {
    EXEC_OK,
    EXEC_MODIFICO_PC,
    EXEC_SYSCALL,
    EXEC_SEG_FAULT //para MOV
} t_resultado_execute;


// --- Funciones ---
//void                ejecutar_proceso(int pid, t_contexto* ctx, int socket_memoria);
char*               fetch_instruccion(int socket_memoria, int pid, int pc);
t_instruccion*      parsear_instruccion(char* linea);
void                liberar_instruccion(t_instruccion* instr);
//t_resultado_execute execute(t_instruccion* instr, t_contexto* ctx);
t_resultado_execute execute(t_instruccion* instr, t_contexto* ctx, int pid, int socket_memoria, uint32_t tam_max_segmento);
//para kscheduler
//op_code ejecutar_proceso(int pid, t_contexto* ctx, int socket_memoria);
op_code ejecutar_proceso(int pid, t_contexto* ctx, int socket_memoria, t_instruccion** ultima_instr);
uint32_t mmu_traducir(t_contexto* ctx, uint32_t dir_logica, uint32_t tamanio, uint32_t tam_max_segmento);

typedef struct
{
    uint32_t id;
    uint32_t base;
    uint32_t limite;
    int socket;
    char ip[50];
    char puerto[20];
} t_memory_stick_cpu;//lo sacamos de cpu.c
extern t_list* lista_memory_sticks; // esto va en CPU.C?
extern pthread_mutex_t mx_memory_sticks;// esto va en CPU.C?
t_memory_stick_cpu* buscar_ms_por_base(uint32_t dir_fisica);


typedef struct {
    int op; 
    int pid;
    int pc;
} t_fetch;

#endif