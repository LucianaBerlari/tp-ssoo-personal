#include "contexto.h"

void inicializar_registros(t_registros *r)
{
    r->PC = 0;

    r->AX = r->BX = r->CX = r->DX = 0;

    r->EAX = r->EBX = r->ECX = r->EDX = 0;

    r->SI = r->DI = 0;
}

// para setear el valor de forma dinámica paso el valor y tengo un puntero
//  reg es el identificador del registro
void set_registro(t_registros *r, t_registro reg, uint32_t valor)
{
    switch (reg)
    {
    case REG_PC:
        r->PC = valor;
        break;
    case REG_AX:
        r->AX = valor;
        break;
    case REG_BX:
        r->BX = valor;
        break;
    case REG_CX:
        r->CX = valor;
        break;
    case REG_DX:
        r->DX = valor;
        break;
    case REG_EAX:
        r->EAX = valor;
        break;
    case REG_EBX:
        r->EBX = valor;
        break;
    case REG_ECX:
        r->ECX = valor;
        break;
    case REG_EDX:
        r->EDX = valor;
        break;
    case REG_SI:
        r->SI = valor;
        break;
    case REG_DI:
        r->DI = valor;
        break;
    }
}
// ahora pedimos el valor del registro
uint32_t get_valor_registro(t_registros *r, t_registro reg)
{
    switch (reg)
    {
    case REG_PC:
        return r->PC;
    case REG_AX:
        return r->AX;
    case REG_BX:
        return r->BX;
    case REG_CX:
        return r->CX;
    case REG_DX:
        return r->DX;
    case REG_EAX:
        return r->EAX;
    case REG_EBX:
        return r->EBX;
    case REG_ECX:
        return r->ECX;
    case REG_EDX:
        return r->EDX;
    case REG_SI:
        return r->SI;
    case REG_DI:
        return r->DI;
    }
    return 0;
}
void incializar_contexto(t_contexto* t)
{
    t->registros.PC = 0;
    t->registros.AX = 0;
    t->registros.BX = 0;
    t->registros.CX = 0;
    t->registros.DX = 0;
    t->registros.EAX = 0;
    t->registros.EBX = 0;
    t->registros.ECX = 0;
    t->registros.EDX = 0;
    t->registros.DI = 0;
    t->registros.SI = 0;
}