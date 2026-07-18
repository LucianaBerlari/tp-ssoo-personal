#ifndef PROTOCOLO_H
#define PROTOCOLO_H

/*typedef enum {
    // CPU → Kernel Memory
    //PEDIR_CONTEXTO=1,
    PEDIDO_CONTEXTO=3, // le puse tres porque como ya hay un enum en utils.h me tira error. Podriamos pasarlo todo ahi?
    FETCH_INSTRUCCION,

    // Kernel Memory → CPU

    RESPUESTA_CONTEXTO,
    RESPUESTA_INSTRUCCION,

    //Kernel Scheduler -> Kernel Memory
    CARGA_PROCESO,
// Kernel Scheduler → CPU (interrupciones)  ← NUEVO
    FIN_QUANTUM,        // = 8, para desalojar por tiempo
    PROCESO_TERMINADO,  // = 9, por si necesitás avisarle a la CPU que pare
} op_code_cpu_km;*/

void *ejecutar_sleep(void *arg);

typedef enum
{
    // Generales
    MENSAJE,
    PAQUETE,

    // CPU → Kernel Memory
    PEDIDO_CONTEXTO,
    FETCH_INSTRUCCION,
    ACTUALIZAR_CONTEXTO,

    // Instrucciones CPU (no syscall)
    NOOP,
    SET,
    MOV_IN,
    MOV_OUT,
    SUM,
    SUB,
    JNZ,
    COPY_MEM,

    // Kernel Memory → CPU

    RESPUESTA_CONTEXTO,
    RESPUESTA_INSTRUCCION,
    SOLICITUD_ESCRITURA,
    SOLICITUD_LECTURA,
    // CPU → Kernel Scheduler (syscalls)
    S_MUTEX_CREATE, // agrego S de syscall
    S_MUTEX_LOCK,
    S_MUTEX_UNLOCK,
    S_MEM_ALLOC,
    S_MEM_FREE,
    S_SLEEP,
    S_STDOUT,
    S_STDIN,
    S_INIT_PROC,
    S_EXIT,
    NUEVO_MEMORY_STICK,

    // Kernel Scheduler → CPU (dispatch e interrupt)
    DESPACHAR_PROCESO, // Scheduler manda PID a CPU
    INTERRUPCION,      // Scheduler manda interrupción a CPU
    FIN_QUANTUM,
    ENVIAR_PID,

    // Motivos de desalojo CPU → Scheduler
    MOTIVO_EXIT,
    MOTIVO_SYSCALL,
    MOTIVO_INTERRUPCION,
    MOTIVO_SEG_FAULT,

    // Kernel Scheduler -> Kernel Memory
    CARGA_PROCESO,
    CONSULTAR_ESPACIO,
    COMPACTACION_RECIBIDA,
    CREAR_SEGMENTO,
    ELIMINAR_SEGMENTO,
    SUSPENDER_PROCESO,
    DESUSPENDER_PROCESO,
    // FINALIZAR_PROCESO,

    MEMORIA_LLENA, // este debería ser de Kernel Memory → Kernel Scheduler ¿?
    MEMORIA_OK,    // este debería ser de Kernel Memory → Kernel Scheduler ¿?

    // Kernel Memory -> CPU / IO
    RESPUESTA_ESCRITURA,
    RESPUESTA_LECTURA,

    FINALIZAR_PROCESO, // este debería ser de Kernel Scheduler -> Kernel Memory ¿?

    // CPU <-> KErnel Scheduler
    PEDIR_ID_CPU,     // El scheduler le pregunta  cpu su numero
    RESPONDER_ID_CPU, // CPu responde su numero

    // KS -> IO
    STDIN,
    STDOUT,
    SLEEP,

    // IO -> KS
    STDIN_RESPUESTA,
    STDOUT_RESPUESTA,
    SLEEP_RESPUESTA,

    // KM -> KS
    ERROR_CREACION_SEGMENTO,
    SEGMENTO_CREADO_CORRECTAMENTE,
    MEMORIA_CORRUPTA,
    AVISO_COMPACTACION,
    OK,
    ERROR,
    CREACION_PROCESO_EXITOSA,

    // CPU / KM -> MS
    SOLICITUD_ESCRITURA_MS,
    SOLICITUD_LECTURA_MS,

    // MS -> CPU / KM
    RESPUESTA_ESCRITURA_MS,
    RESPUESTA_LECTURA_MS,

    // KM -> SWAP
    ESCRITURA_SWAP,
    LECTURA_SWAP,

    //SWAP -> KM 
    RESPUESTA_LECTURA_SW,
    SUSPENSION_ERROR,
    SUSPENSION_OK,

} op_code;

#endif
