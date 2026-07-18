#ifndef CONTEXTO_H_
#define CONTEXTO_H_
#include <stdint.h> // ESpacios de datos
#include <commons/collections/list.h> // listas
typedef struct {

uint32_t PC;

uint8_t AX, BX, CX, DX;

uint32_t EAX, EBX, ECX, EDX;

uint32_t SI, DI;

} t_registros;


typedef struct {
    uint32_t id; // id del hueco --> del MS al que pertenece
    uint32_t base;
    uint32_t limite;
} t_segmento, t_hueco; // t_memory_stick;
typedef struct {
    uint32_t id;
    uint32_t base;
    uint32_t limite;
    int socket;
    char ip[50];
    char puerto[20];
} t_memory_stick;

typedef struct {
    t_registros registros;
    t_list* tabla_segmentos; // sería una lista de t_segmento* ¿?
    
} t_contexto;

// Enum de registros
typedef enum {
    REG_PC,
    REG_AX,
    REG_BX,
    REG_CX,
    REG_DX,
    REG_EAX,
    REG_EBX,
    REG_ECX,
    REG_EDX,
    REG_SI,
    REG_DI
} t_registro;


// Funciones
void inicializar_registros(t_registros* r);
void set_registro(t_registros* r, t_registro reg, uint32_t valor);
uint32_t get_valor_registro(t_registros* r, t_registro reg);

// Funciones de contexto
void incializar_contexto(t_contexto* t);
#endif