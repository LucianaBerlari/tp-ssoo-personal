#include "ciclo.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <utils/protocolo.h>
#include <pthread.h>
#include <sys/socket.h>// para el MSG_WAITALL del recv dentro de fetch
extern uint32_t tam_max_segmento; // esperamos recibir de KMemory


op_code ejecutar_proceso(int pid, t_contexto* ctx, int socket_memoria, t_instruccion** ultima_instr) {
    printf("▶ CPU ejecutando PID %d\n", pid);

    while (1) {
        // 1. FETCH
        char* linea = fetch_instruccion(socket_memoria, pid, ctx->registros.PC);
        //printf("FETCH PC=%d: %s\n", ctx->registros.PC, linea);
        log_info(logger, "## PID: %d - FETCH - Program Counter: %d", pid, ctx->registros.PC);

        // 2. DECODE
        t_instruccion* instr = parsear_instruccion(linea);
        free(linea);

        if (instr == NULL) return MOTIVO_EXIT;

        // 3. EXECUTE // agregamos log obligatorio faltante 1 por instrucción
        //log_info(logger, "## PID: %d - Ejecutando: %s - %s", pid, nombre_instruccion(instr),
        // parametros_instruccion(instr));
        loggear_instruccion(instr, pid);
        //t_resultado_execute resultado = execute(instr, ctx);
        t_resultado_execute resultado = execute(instr, ctx, pid, socket_memoria, tam_max_segmento);

        if (resultado == EXEC_SEG_FAULT) {
            liberar_instruccion(instr);
           return MOTIVO_SEG_FAULT; // hay que agregar al protocolo
        }
        // 4. PC++
        if (resultado != EXEC_MODIFICO_PC) {
            ctx->registros.PC++;
        }

        //bool termino    = (instr->tipo == EXIT_PROC);
        bool termino = (instr->tipo == S_EXIT);
        bool es_syscall = (resultado == EXEC_SYSCALL);

        if (termino) {
            liberar_instruccion(instr);
            return MOTIVO_EXIT;
        }

        if (es_syscall) {
            *ultima_instr = instr; // guardamos sin liberar
            return MOTIVO_SYSCALL;
        }

        liberar_instruccion(instr);

        // 5. CHECK INTERRUPT
        pthread_mutex_lock(&mutex_interrupcion);
        if (hay_interrupcion && pid_interrupcion == pid) {
            hay_interrupcion = false;
            pid_interrupcion = -1;
            pthread_mutex_unlock(&mutex_interrupcion);
            log_info(logger, "Interrupción recibida para PID %d", pid);
            log_info(logger, "## Interrupción recibida");//este es el obligatorio ...
            return MOTIVO_INTERRUPCION;
        }
        pthread_mutex_unlock(&mutex_interrupcion);
    }
}

//esto debería venir de k_memory del archivo de pseudocodigo, ya no hace falta?
// ahora con paquete? de k_memory
char* fetch_instruccion(int socket, int pid, int pc)
{
    log_debug(logger, "Pedi la instruccion a KM");
    t_paquete* paquete = crear_paquete(FETCH_INSTRUCCION);
    agregar_a_paquete(paquete, &pid, sizeof(int));
    agregar_a_paquete(paquete, &pc, sizeof(int));
    enviar_paquete(paquete, socket);
    eliminar_paquete(paquete);

    // 📥 recibir respuesta
    int cod_op = recibir_operacion(socket);
    if (cod_op != RESPUESTA_INSTRUCCION) {
        printf("Error en FETCH\n");
        return strdup("EXIT");
    }
    t_list* lista = recibir_paquete(socket);
    char* instruccion = strdup(list_get(lista, 0)); // copiamos
    list_destroy_and_destroy_elements(lista, free); // liberamos
    return instruccion;
}

// FETCH (ya lo tenés bien)
// PARSE

t_instruccion* parsear_instruccion(char* linea) {
    t_instruccion* instr = malloc(sizeof(t_instruccion));
    instr->cant_args = 0;
    for (int i = 0; i < 3; i++) instr->args[i] = NULL;

    char* copia = strdup(linea);

    copia[strcspn(copia, "\r\n")] = 0;


    char* token = strtok(copia, " ");

    if (token == NULL) {
        free(copia);
        free(instr);
        return NULL;
    }

    if      (strcmp(token, "NOOP")         == 0) instr->tipo = NOOP;
    else if (strcmp(token, "SET")          == 0) instr->tipo = SET;
    else if (strcmp(token, "SUM")          == 0) instr->tipo = SUM;
    else if (strcmp(token, "SUB")          == 0) instr->tipo = SUB;
    else if (strcmp(token, "JNZ")          == 0) instr->tipo = JNZ;
    else if (strcmp(token, "MOV_IN")       == 0) instr->tipo = MOV_IN;
    else if (strcmp(token, "MOV_OUT")      == 0) instr->tipo = MOV_OUT;
    else if (strcmp(token, "COPY_MEM")     == 0) instr->tipo = COPY_MEM;
    /* antes las sicalls estaban sin S_ 
    else if (strcmp(token, "EXIT")         == 0) instr->tipo = EXIT_PROC; */
    else if (strcmp(token, "MUTEX_CREATE") == 0) instr->tipo = S_MUTEX_CREATE;
    else if (strcmp(token, "MUTEX_LOCK")   == 0) instr->tipo = S_MUTEX_LOCK;
    else if (strcmp(token, "MUTEX_UNLOCK") == 0) instr->tipo = S_MUTEX_UNLOCK;
    else if (strcmp(token, "MEM_ALLOC")    == 0) instr->tipo = S_MEM_ALLOC;
    else if (strcmp(token, "MEM_FREE")     == 0) instr->tipo = S_MEM_FREE;
    else if (strcmp(token, "SLEEP")        == 0) instr->tipo = S_SLEEP;
    else if (strcmp(token, "STDOUT")       == 0) instr->tipo = S_STDOUT;
    else if (strcmp(token, "STDIN")        == 0) instr->tipo = S_STDIN;
    else if (strcmp(token, "INIT_PROC")    == 0) instr->tipo = S_INIT_PROC;
    else if (strcmp(token, "EXIT")         == 0) instr->tipo = S_EXIT;
    else {
        printf("Instrucción desconocida: %s\n", token);
        free(copia);
        free(instr);
        return NULL;
    }

    token = strtok(NULL, " ");
    while (token != NULL && instr->cant_args < 3) {
        instr->args[instr->cant_args++] = strdup(token);
        token = strtok(NULL, " ");
    }

    free(copia);
    return instr;
}

// HELPERS DE REGISTROS

void* obtener_registro(t_contexto* ctx, char* nombre) {
    if (strcmp(nombre, "PC")  == 0) return &ctx->registros.PC;
    if (strcmp(nombre, "AX")  == 0) return &ctx->registros.AX;
    if (strcmp(nombre, "BX")  == 0) return &ctx->registros.BX;
    if (strcmp(nombre, "CX")  == 0) return &ctx->registros.CX;
    if (strcmp(nombre, "DX")  == 0) return &ctx->registros.DX;
    if (strcmp(nombre, "EAX") == 0) return &ctx->registros.EAX;
    if (strcmp(nombre, "EBX") == 0) return &ctx->registros.EBX;
    if (strcmp(nombre, "ECX") == 0) return &ctx->registros.ECX;
    if (strcmp(nombre, "EDX") == 0) return &ctx->registros.EDX;
    if (strcmp(nombre, "SI")  == 0) return &ctx->registros.SI;
    if (strcmp(nombre, "DI")  == 0) return &ctx->registros.DI;
    return NULL;
}

size_t tamanio_registro(char* nombre) {
    if (strcmp(nombre, "AX") == 0 || strcmp(nombre, "BX") == 0 ||
        strcmp(nombre, "CX") == 0 || strcmp(nombre, "DX") == 0)
        return sizeof(uint8_t);
    return sizeof(uint32_t);
}

uint32_t leer_registro(t_contexto* ctx, char* nombre) {
    void* ptr = obtener_registro(ctx, nombre);
    if (tamanio_registro(nombre) == sizeof(uint8_t))
        return (uint32_t)(*(uint8_t*)ptr);
    return *(uint32_t*)ptr;
}

void escribir_registro(t_contexto* ctx, char* nombre, uint32_t valor) {
    void* ptr = obtener_registro(ctx, nombre);
    if (tamanio_registro(nombre) == sizeof(uint8_t))
        *(uint8_t*)ptr = (uint8_t)valor;
    else
        *(uint32_t*)ptr = valor;
}

// EXECUTE

// Retorna true si es syscall (hay que pausar el ciclo)
// Retorna false si modifica el PC (JNZ tomado)
// El caller incrementa PC solo si ninguno de los dos aplica
//t_resultado_execute execute(t_instruccion* instr, t_contexto* ctx) {
t_resultado_execute execute(t_instruccion* instr, t_contexto* ctx, int pid, int socket_memoria, uint32_t tam_max_segmento) {
    switch (instr->tipo) {

        case NOOP:
            printf("EXEC: NOOP\n");
            return EXEC_OK;

        case SET: {
            // SET <Registro> <Valor>
            uint32_t valor = (uint32_t)atoi(instr->args[1]);
            escribir_registro(ctx, instr->args[0], valor);
            printf("EXEC: SET %s = %u\n", instr->args[0], valor);
            return EXEC_OK;
        }

        case SUM: {
            // SUM <Destino> <Origen>
            uint32_t a = leer_registro(ctx, instr->args[0]);
            uint32_t b = leer_registro(ctx, instr->args[1]);
            escribir_registro(ctx, instr->args[0], a + b);
            printf("EXEC: SUM %s(%u) + %s(%u) = %u\n",
                instr->args[0], a, instr->args[1], b, a + b);
            return EXEC_OK;
        }

        case SUB: {
            uint32_t a = leer_registro(ctx, instr->args[0]);
            uint32_t b = leer_registro(ctx, instr->args[1]);
            escribir_registro(ctx, instr->args[0], a - b);
            printf("EXEC: SUB %s = %u\n", instr->args[0], a - b);
            return EXEC_OK;
        }

        case JNZ: {
            // JNZ <Registro> <Instruccion>
            uint32_t val = leer_registro(ctx, instr->args[0]);
            if (val != 0) {
                ctx->registros.PC = (uint32_t)atoi(instr->args[1]);
                printf("EXEC: JNZ tomado -> PC=%u\n", ctx->registros.PC);
                return EXEC_MODIFICO_PC; // saltó, no incrementar PC
            }
            printf("EXEC: JNZ no tomado\n");
            return EXEC_OK; // no saltó, incrementar PC normal
        }
        // falta probar los MOV
        case MOV_IN: {
    uint32_t dir_logica = leer_registro(ctx, "SI");
    uint32_t dir_fisica = mmu_traducir(ctx, dir_logica, sizeof(uint32_t), tam_max_segmento);
    if (dir_fisica == UINT32_MAX) return EXEC_SEG_FAULT;

    t_memory_stick_cpu* ms = buscar_ms_por_base(dir_fisica);
    if (ms == NULL) {
        log_error(logger, "No se encontró Memory Stick para dir_fisica=%u", dir_fisica);
        return EXEC_SEG_FAULT;
    }

    uint32_t dir_local = dir_fisica - ms->base;
    uint32_t tamanio = sizeof(uint32_t);

    t_paquete* paquete = crear_paquete(SOLICITUD_LECTURA_MS);
    agregar_a_paquete(paquete, &dir_local, sizeof(uint32_t));
    agregar_a_paquete(paquete, &tamanio, sizeof(uint32_t));
    enviar_paquete(paquete, ms->socket);
    eliminar_paquete(paquete);

    int cod_op = recibir_operacion(ms->socket);
    if (cod_op != RESPUESTA_LECTURA_MS) {
        log_error(logger, "Error en lectura del Memory Stick %d", ms->id);
        return EXEC_OK;
    }

    t_list* lista = recibir_paquete(ms->socket);
    uint32_t valor;
    memcpy(&valor, list_get(lista, 0), sizeof(uint32_t));
    list_destroy_and_destroy_elements(lista, free);

    escribir_registro(ctx, instr->args[0], valor);
    log_info(logger, "PID: %d - Acción: LEER - Dirección Física: %u - Valor: %u", pid, dir_fisica, valor);
    return EXEC_OK;
}

case MOV_OUT: {
    uint32_t dir_logica = leer_registro(ctx, "DI");
    uint32_t dir_fisica = mmu_traducir(ctx, dir_logica, sizeof(uint32_t), tam_max_segmento);
    if (dir_fisica == UINT32_MAX) return EXEC_SEG_FAULT;

    t_memory_stick_cpu* ms = buscar_ms_por_base(dir_fisica);
    if (ms == NULL) {
        log_error(logger, "No se encontró Memory Stick para dir_fisica=%u", dir_fisica);
        return EXEC_SEG_FAULT;
    }

    uint32_t dir_local = dir_fisica - ms->base;
    uint32_t valor = leer_registro(ctx, instr->args[0]);
    uint32_t tamanio = sizeof(uint32_t);

    t_paquete* paquete = crear_paquete(SOLICITUD_ESCRITURA_MS);
    agregar_a_paquete(paquete, &dir_local, sizeof(uint32_t));
    agregar_a_paquete(paquete, &tamanio, sizeof(uint32_t));
    agregar_a_paquete(paquete, &valor, sizeof(uint32_t));
    enviar_paquete(paquete, ms->socket); //ahora socket de ms
    eliminar_paquete(paquete);

    int respuesta;
    recv(ms->socket, &respuesta, sizeof(int), MSG_WAITALL); // escritura responde con int crudo, sin paquete
    if (respuesta != OK) {
        log_error(logger, "Memory Stick %d informó error de escritura", ms->id);
    }

    log_info(logger, "PID: %d - Acción: ESCRIBIR - Dirección Física: %u - Valor: %u", pid, dir_fisica, valor);
    return EXEC_OK;
}

case COPY_MEM: {
    uint32_t bytes_a_copiar = leer_registro(ctx, instr->args[0]);
    uint32_t dir_origen  = leer_registro(ctx, "SI");
    uint32_t dir_destino = leer_registro(ctx, "DI");

    uint32_t fis_origen  = mmu_traducir(ctx, dir_origen,  bytes_a_copiar, tam_max_segmento);
    uint32_t fis_destino = mmu_traducir(ctx, dir_destino, bytes_a_copiar, tam_max_segmento);
    if (fis_origen == UINT32_MAX || fis_destino == UINT32_MAX) return EXEC_SEG_FAULT;

    t_memory_stick_cpu* ms_origen  = buscar_ms_por_base(fis_origen);
    t_memory_stick_cpu* ms_destino = buscar_ms_por_base(fis_destino);
    if (ms_origen == NULL || ms_destino == NULL) {
        log_error(logger, "No se encontró Memory Stick para COPY_MEM");
        return EXEC_SEG_FAULT;
    }
    // Nota: si bytes_a_copiar cruza el límite del stick de origen o destino,
    // habría que partir la solicitud en 2 (no cubierto acá; avisame si lo necesitás).

    uint32_t local_origen  = fis_origen  - ms_origen->base;
    uint32_t local_destino = fis_destino - ms_destino->base;

    // 1. leer del origen
    t_paquete* p_leer = crear_paquete(SOLICITUD_LECTURA_MS);
    agregar_a_paquete(p_leer, &local_origen, sizeof(uint32_t));
    agregar_a_paquete(p_leer, &bytes_a_copiar, sizeof(uint32_t));
    enviar_paquete(p_leer, ms_origen->socket);
    eliminar_paquete(p_leer);

    int cod_op = recibir_operacion(ms_origen->socket);
    if (cod_op != RESPUESTA_LECTURA_MS) {
        log_error(logger, "Error en lectura del Memory Stick %d (COPY_MEM)", ms_origen->id);
        return EXEC_OK;
    }
    t_list* lista = recibir_paquete(ms_origen->socket);
    void* datos = list_get(lista, 0);

    // 2. escribir en el destino
    t_paquete* p_escribir = crear_paquete(SOLICITUD_ESCRITURA_MS);
    agregar_a_paquete(p_escribir, &local_destino, sizeof(uint32_t));
    agregar_a_paquete(p_escribir, &bytes_a_copiar, sizeof(uint32_t));
    agregar_a_paquete(p_escribir, datos, bytes_a_copiar);
    enviar_paquete(p_escribir, ms_destino->socket);
    eliminar_paquete(p_escribir);

    list_destroy_and_destroy_elements(lista, free);

    int respuesta;
    recv(ms_destino->socket, &respuesta, sizeof(int), MSG_WAITALL);
    if (respuesta != OK) {
        log_error(logger, "Memory Stick %d informó error de escritura (COPY_MEM)", ms_destino->id);
    }

    log_info(logger, "PID: %d - COPY_MEM origen=%u destino=%u bytes=%u",
        pid, fis_origen, fis_destino, bytes_a_copiar);
    return EXEC_OK;
}

        /*case MOV_IN: {
            // Lee de memoria[SI] y guarda en Registro Datos
            uint32_t dir_logica = leer_registro(ctx, "SI");
            uint32_t dir_fisica = mmu_traducir(ctx, dir_logica, sizeof(uint32_t), tam_max_segmento);

            if (dir_fisica == UINT32_MAX) return EXEC_SEG_FAULT;

            // pedir lectura a KMemory
            t_paquete* paquete = crear_paquete(SOLICITUD_LECTURA);
            agregar_a_paquete(paquete, &pid, sizeof(int));
            agregar_a_paquete(paquete, (uint32_t[]){sizeof(uint32_t)}, sizeof(uint32_t));
            agregar_a_paquete(paquete, &dir_fisica, sizeof(uint32_t));
            enviar_paquete(paquete, socket_memoria);
            eliminar_paquete(paquete);

            // recibir valor
            int cod_op = recibir_operacion(socket_memoria);
            if (cod_op != RESPUESTA_LECTURA) return EXEC_OK;

            t_list* lista = recibir_paquete(socket_memoria);
            uint32_t valor = *(uint32_t*)list_get(lista, 0);
            list_destroy_and_destroy_elements(lista, free);

            escribir_registro(ctx, instr->args[0], valor);

            log_info(logger, "PID: %d - Acción: LEER - Dirección Física: %d - Valor: %d",
                pid, dir_fisica, valor);
            return EXEC_OK;
        }

        case MOV_OUT: {
            // Escribe valor del Registro Datos en memoria[DI]
            uint32_t dir_logica = leer_registro(ctx, "DI");
            uint32_t dir_fisica = mmu_traducir(ctx, dir_logica, sizeof(uint32_t), tam_max_segmento);

            if (dir_fisica == UINT32_MAX) return EXEC_SEG_FAULT;

            //agregado de prueba
            log_info(logger,"arg0 = %s", instr->args[0]);

            uint32_t valor = leer_registro(ctx, instr->args[0]);

            // mandar escritura a KMemory
            t_paquete* paquete = crear_paquete(SOLICITUD_ESCRITURA);
            agregar_a_paquete(paquete, &pid, sizeof(int));
            agregar_a_paquete(paquete, (uint32_t[]){sizeof(uint32_t)}, sizeof(uint32_t));
            //uint32_t tamanio = sizeof(uint32_t)
            //agregar_a_paquete(paquete, &tamanio, sizeof(uint32_t));
            agregar_a_paquete(paquete, &valor, sizeof(uint32_t));
            agregar_a_paquete(paquete, &dir_fisica, sizeof(uint32_t));
            enviar_paquete(paquete, socket_memoria);
            eliminar_paquete(paquete);  

            int cod_op = recibir_operacion(socket_memoria);
            //recibimos ok?
            if (cod_op != RESPUESTA_ESCRITURA){
            log_error(logger, "Respuesta inesperada de Memoria. Recibido: %d", cod_op);
            return EXEC_OK;
            }

            t_list* lista = recibir_paquete(socket_memoria);

            int ok = *(int*)list_get(lista, 0);

            if (ok != OK){
            log_error(logger, "Memoria informó error de escritura");
            }

            list_destroy_and_destroy_elements(lista, free);

            log_info(logger, "PID: %d - Acción: ESCRIBIR - Dirección Física: %u - Valor: %u", pid, dir_fisica, valor);
            return EXEC_OK;
        }

        case COPY_MEM: {
            // COPY_MEM <Registro Tamaño>
            uint32_t bytes_a_copiar = leer_registro(ctx, instr->args[0]);
            uint32_t dir_origen  = leer_registro(ctx, "SI");
            uint32_t dir_destino = leer_registro(ctx, "DI");

            // traducir origen y destino con MMU
            uint32_t fis_origen  = mmu_traducir(ctx, dir_origen,  bytes_a_copiar, tam_max_segmento);
            uint32_t fis_destino = mmu_traducir(ctx, dir_destino, bytes_a_copiar, tam_max_segmento);

            if (fis_origen == UINT32_MAX || fis_destino == UINT32_MAX) return EXEC_SEG_FAULT;

            // leer bytes del origen
            t_paquete* p_leer = crear_paquete(SOLICITUD_LECTURA);
            agregar_a_paquete(p_leer, &pid, sizeof(int));
            agregar_a_paquete(p_leer, &bytes_a_copiar, sizeof(uint32_t));
            agregar_a_paquete(p_leer, &fis_origen, sizeof(uint32_t));
            enviar_paquete(p_leer, socket_memoria);
            eliminar_paquete(p_leer);

            int cod_op = recibir_operacion(socket_memoria);
            if (cod_op != RESPUESTA_LECTURA) return EXEC_OK;

            t_list* lista = recibir_paquete(socket_memoria);
            // asumimos que KMemory manda los bytes como un bloque
            void* datos = list_get(lista, 0);

            // escribir bytes en el destino
            t_paquete* p_escribir = crear_paquete(SOLICITUD_ESCRITURA);
            agregar_a_paquete(p_escribir, &pid, sizeof(int));
            agregar_a_paquete(p_escribir, &bytes_a_copiar, sizeof(uint32_t));
            agregar_a_paquete(p_escribir, datos, bytes_a_copiar);
            agregar_a_paquete(p_escribir, &fis_destino, sizeof(uint32_t));
            enviar_paquete(p_escribir, socket_memoria);
            eliminar_paquete(p_escribir);

            list_destroy_and_destroy_elements(lista, free);

            int cod_op2 = recibir_operacion(socket_memoria);
            if (cod_op2 == RESPUESTA_ESCRITURA) {
                t_list* lista2 = recibir_paquete(socket_memoria);
                list_destroy_and_destroy_elements(lista2, free);
            }

            log_info(logger, "PID: %d - COPY_MEM origen=%d destino=%d bytes=%d",
                pid, fis_origen, fis_destino, bytes_a_copiar);
            return EXEC_OK;
        }*/

        // Syscalls: avisamos al caller que debe interrumpir
        /*case MUTEX_CREATE:
        case MUTEX_LOCK:
        case MUTEX_UNLOCK:
        case MEM_ALLOC:
        case MEM_FREE:
        case SLEEP:
        case STDOUT_OP:
        case STDIN_OP:
        case INIT_PROC:
        case EXIT_PROC:
            printf("EXEC: SYSCALL %d\n", instr->tipo);
            return EXEC_SYSCALL;*/ // es syscall, el ciclo debe pausarse
        case S_MUTEX_CREATE:
        case S_MUTEX_LOCK:
        case S_MUTEX_UNLOCK:
        case S_MEM_ALLOC:
        case S_MEM_FREE:
        case S_SLEEP:
        case S_STDOUT:
        case S_STDIN:
        case S_INIT_PROC:
        case S_EXIT:
            printf("EXEC: SYSCALL %d\n", instr->tipo);
            return EXEC_SYSCALL;
        default:
            printf("EXEC: instrucción no implementada\n");
            return EXEC_OK;
    }
}

void liberar_instruccion(t_instruccion* instr) {
    for (int i = 0; i < instr->cant_args; i++)
        free(instr->args[i]);
    free(instr);
}

//falta t_contexto y t_segmento
uint32_t mmu_traducir(t_contexto* ctx, uint32_t dir_logica, uint32_t tamanio, uint32_t tam_max_segmento)
{
    uint32_t num_segmento = dir_logica / tam_max_segmento;
    uint32_t desplazamiento = dir_logica % tam_max_segmento;

    t_segmento* seg = NULL;
    for (int i = 0; i < list_size(ctx->tabla_segmentos); i++) {
        t_segmento* s = list_get(ctx->tabla_segmentos, i);
        if (s->id == num_segmento) {
            seg = s;
            break;
        }
    }

    if (seg == NULL) {
        log_error(logger, "SEGMENTO NO ENCONTRADO - num_segmento=%u (dir_logica=%u)", num_segmento, dir_logica);
        return UINT32_MAX;
    }

    if (desplazamiento + tamanio > seg->limite) {
        log_error(logger, "SEG_FAULT - desplazamiento=%u + tamanio=%u > limite=%u (seg_id=%d)",
                  desplazamiento, tamanio, seg->limite, seg->id);
        return UINT32_MAX;
    }

    uint32_t dir_fisica = seg->base + desplazamiento;
    log_info(logger, "MMU CALC: dir_logica=%u -> seg_id=%d base=%u limite=%u desplazamiento=%u dir_fisica=%u",
             dir_logica, seg->id, seg->base, seg->limite, desplazamiento, dir_fisica);
    return dir_fisica;
}

void loggear_instruccion(t_instruccion *instr, int pid)
{
    switch(instr->tipo)
    {
        case NOOP:
            log_info(logger, "## PID: %d - Ejecutando: NOOP", pid);
            break;

        case SET:
            log_info(logger, "## PID: %d - Ejecutando: SET - %s %s",
                     pid, instr->args[0], instr->args[1]);
            break;

        case SUM:
            log_info(logger, "## PID: %d - Ejecutando: SUM - %s %s",
                     pid, instr->args[0], instr->args[1]);
            break;

        case SUB:
            log_info(logger, "## PID: %d - Ejecutando: SUB - %s %s",
                     pid, instr->args[0], instr->args[1]);
            break;

        case JNZ:
            log_info(logger, "## PID: %d - Ejecutando: JNZ - %s %s",
                     pid, instr->args[0], instr->args[1]);
            break;

        case S_MEM_ALLOC:
            log_info(logger, "## PID: %d - Ejecutando: MEM_ALLOC - %s %s",
                     pid, instr->args[0], instr->args[1]);
            break;

        case MOV_IN:
            log_info(logger, "## PID: %d - Ejecutando: MOV_IN - %s", pid, instr->args[0]);
            break;

        case MOV_OUT:
            log_info(logger, "## PID: %d - Ejecutando: MOV_OUT - %s", pid, instr->args[0]);
            break;

        case COPY_MEM:
            log_info(logger, "## PID: %d - Ejecutando: COPY_MEM - %s",
             pid, instr->args[0]);
            break;

        case S_MEM_FREE:
            log_info(logger, "## PID: %d - Ejecutando: MEM_FREE - %s", pid, instr->args[0]);
            break;

        case S_SLEEP:
            log_info(logger, "## PID: %d - Ejecutando: SLEEP - %s", pid, instr->args[0]);
            break;

        case S_STDOUT:
            log_info(logger, "## PID: %d - Ejecutando: STDOUT - %s %s", pid, instr->args[0], instr->args[1]);
            break;

        case S_STDIN:
            log_info(logger, "## PID: %d - Ejecutando: STDIN - %s %s", pid, instr->args[0], instr->args[1]);
            break;

        case S_MUTEX_CREATE:
            log_info(logger, "## PID: %d - Ejecutando: MUTEX_CREATE - %s", pid, instr->args[0]);
            break;

        case S_MUTEX_LOCK:
            log_info(logger, "## PID: %d - Ejecutando: MUTEX_LOCK - %s", pid, instr->args[0]);
            break;

        case S_MUTEX_UNLOCK:
            log_info(logger, "## PID: %d - Ejecutando: MUTEX_UNLOCK - %s", pid, instr->args[0]);
            break;

        case S_INIT_PROC:
            log_info(logger, "## PID: %d - Ejecutando: INIT_PROC - %s %s", pid, instr->args[0], instr->args[1]);
            break;

        case S_EXIT:
            log_info(logger, "## PID: %d - Ejecutando: EXIT", pid);
            break;
        default:
            log_warning(logger, "## PID: %d - Instrucción no contemplada (tipo=%d)",
        pid, instr->tipo);
    }
}

// borramos las 2 versiones anteriores de MMU?
/*uint32_t mmu_traducir(t_contexto* ctx, uint32_t dir_logica, uint32_t tamanio, uint32_t tam_max_segmento)
{
    t_segmento* seg = NULL;

    // 1. Buscar segmento que contiene la dirección lógica
    for (int i = 0; i < list_size(ctx->tabla_segmentos); i++) {
        t_segmento* s = list_get(ctx->tabla_segmentos, i);

        if (dir_logica >= s->base && dir_logica < (s->base + s->limite)) {
            seg = s;
            break;
        }
    }
    // 2. Validación de existencia del segmento
    if (seg == NULL) {
        log_error(logger,
                  "SEGMENTO NO ENCONTRADO - dir_logica=%u",
                  dir_logica);
        return UINT32_MAX;
    }
    // 3. Calcular desplazamiento dentro del segmento
    uint32_t desplazamiento = dir_logica - seg->base;

    // 4. Validar límite del segmento (segfault)
    if (desplazamiento + tamanio > seg->limite) {
        log_error(logger,"SEG_FAULT - desplazamiento=%u + tamanio=%u > limite=%u (seg_id=%d)",
                  desplazamiento, tamanio, seg->limite, seg->id);
        return UINT32_MAX;
    }

    // 5. Dirección física final
    uint32_t dir_fisica = seg->base + desplazamiento;

    log_info(logger, "MMU CALC: dir_logica=%u -> seg_id=%d base=%u limite=%u desplazamiento=%u dir_fisica=%u",
             dir_logica, seg->id, seg->base, seg->limite, desplazamiento, dir_fisica);

    return dir_fisica;
}*/

/*uint32_t mmu_traducir(t_contexto* ctx, uint32_t dir_logica, uint32_t tamanio, uint32_t tam_max_segmento) {
    // 1. calcular número de segmento y desplazamiento
    //uint32_t num_segmento   = dir_logica / tam_max_segmento;
    t_segmento* seg = NULL;

for (int i = 0; i < list_size(ctx->tabla_segmentos); i++) {
    t_segmento* s = list_get(ctx->tabla_segmentos, i);

    if (dir_logica >= s->base && dir_logica < s->base + s->limite) {
        seg = s;
        break;
    }
}
uint32_t desplazamiento = dir_logica - seg->base;
    //uint32_t desplazamiento = dir_logica % tam_max_segmento;
    //log_info(logger, "MMU CALC: dir_logica=%u -> segmento=%u desplazamiento=%u", dir_logica, seg, desplazamiento);
    log_info(logger,"MMU CALC: dir_logica=%u -> seg_base=%u seg_limite=%u desplazamiento=%u", dir_logica, seg->base, seg->limite, desplazamiento);
    // 2. buscar el segmento en la tabla
    /*t_segmento* seg = NULL;
    for (int i = 0; i < list_size(ctx->tabla_segmentos); i++) {
        t_segmento* s = list_get(ctx->tabla_segmentos, i);
        if (s->id == num_segmento) {
            seg = s;
            break;
        }
    }

    if (seg == NULL) {
        log_error(logger, "Segmento %d no encontrado", seg);
        return UINT32_MAX; // señal de error
    }

    // 3. verificar que no se sale del segmento
    if (desplazamiento + tamanio > seg->limite) {
        log_error(logger, "SEG_FAULT: desplazamiento %d + tamanio %d > limite %d",
            desplazamiento, tamanio, seg->limite);
        return UINT32_MAX; // señal de SEG_FAULT
    }

    // 4. dirección física = base del segmento + desplazamiento
    uint32_t dir_fisica = seg->base + desplazamiento;
    return dir_fisica;
}*/
/*void ejecutar_proceso(int pid, t_contexto* ctx, int socket_memoria)
{
    printf("▶ CPU ejecutando PID %d\n", pid);

    while (1)
    {
        
        // FETCH
        
        char* instruccion = fetch_instruccion(
            socket_memoria,
            pid,
            //tx->PC
            ctx->registros.PC
        );

        printf("FETCH: %s\n", instruccion);

        
        // EXIT CHECK
        
        if (strcmp(instruccion, "EXIT") == 0)
        {
            free(instruccion);
            printf("PID %d finalizado\n", pid);
            break;
        }

        
        // DECODE + EXECUTE (mínimo)
        
        if (strcmp(instruccion, "NOOP") == 0)
        {
            printf("EXEC: NOOP\n");
        }
        else if (strcmp(instruccion, "SET AX 5") == 0)
        {
            //ctx->AX = 5;
            ctx->registros.AX = 5;
            printf("EXEC: SET AX 5\n");
        }
        else if (strcmp(instruccion, "SUM AX BX") == 0)
        {
            //ctx->AX += ctx->BX;
            ctx->registros.AX += ctx->registros.BX;
            printf("EXEC: SUM AX BX -> AX=%d\n", ctx->registros.AX);
        }
        else
        {
            printf("Instrucción no reconocida: %s\n", instruccion);
        }
        
        // PC++
        
        //ctx->PC++;
        ctx->registros.PC++;

        free(instruccion);
    }
}*/