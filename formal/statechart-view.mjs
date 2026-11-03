const values = value => Array.isArray(value) ? value : Object.values(value ?? {});
const set = value => '{' + values(value).join(',') + '}';
const yes = value => value ? 'sí' : 'no';

export function state(v) {
  if ('retained' in v) {
    return `${v.connected ? 'Conectado' : 'Desconectado'}; sesión ${v.generation}; ` +
      `aceptados ${set(v.accepted)}; respaldo ${set(v.retained)}; aplicados ${set(v.applied)}; ` +
      `${v.phase === 'replay' ? 'replay' : 'libre'} ${set(v.snapshot)} paso ${v.position}; ` +
      `envíos ${values(v.network).map(item => item.seq).join(',')}; acks ${set(v.acknowledgements)}`;
  }
  if ('wal' in v) {
    return `${v.opened ? 'Abierto' : 'Sin abrir'}; ${v.phase}; resultado ${v.result}; ` +
      `WAL ${set(v.wal)}; checkpoint ${set(v.checkpoint)}; aceptados ${set(v.accepted)}; ` +
      `bloqueado ${yes(v.halted)}; mantenimiento intentado ${yes(v.maintenanceAttempted)}`;
  }
  if ('watermarkA' in v) {
    return `Paso ${v.phase}; fila ${v.row}; secuencias A=${v.watermarkA}, B=${v.watermarkB}; ` +
      `deletes aplicados ${v.deleteApplications}; publicaciones aplicadas ${v.publishApplications}`;
  }
  return `${v.connected ? 'Conectado' : 'Desconectado'}; detenido ${yes(v.stopped)}; ` +
    `intento ${v.active ? v.owner : 'ninguno'}; trabajador ${v.worker}; ` +
    `pendiente ${yes(v.pending)}; callback ${yes(v.notification)}; entrega ${yes(v.handoff)}; ` +
    `llamada antigua procesada ${yes(v.staleDone)}; admitida ${yes(v.staleTouched)}; ` +
    `sesión reemplazada ${yes(v.replacedSession)}`;
}

const names = {
  Publish: 'Retener publicación nueva', Connect: 'Abrir nueva sesión', Disconnect: 'Perder conexión',
  StartReplay: 'Tomar copia del respaldo', SendReplay: 'Enviar siguiente mensaje del respaldo',
  AbortReplay: 'Abortar replay de otra sesión', FinishReplay: 'Terminar replay',
  Deliver: 'Aplicar mensaje en broker', PersistedAck: 'Recibir ack acumulado',
  Open: 'Abrir y reparar cola incompleta', StartAppend: 'Escribir registro WAL',
  CommitAppend: 'Confirmar retención', RollbackAppend: 'Revertir append rechazado',
  UncertainAppend: 'Conservar evidencia incierta', CheckpointSuccess: 'Completar checkpoint',
  CheckpointFailure: 'Fallar checkpoint posterior', AttemptMaintenance: 'Intentar mantenimiento',
  Reopen: 'Reabrir store', StartNew: 'Iniciar conexión explícita', LogonSuccess: 'Completar logon',
  LoseSocket: 'Perder socket', HandleNotification: 'Procesar desconexión',
  CompleteSuccess: 'Terminar intento correcto', TerminalFailure: 'Terminar intento con error',
  LateCallback: 'Llegar callback atrasado', CheckNotification: 'Comprobar sesión tras listener',
  ScheduleWorker: 'Programar trabajador', DropObsoleteWorker: 'Liberar reserva obsoleta',
  RunWorker: 'Entrar trabajador a conexión', FinishWorker: 'Liberar trabajador anterior',
  DispatchHandoff: 'Entregar reconexión pendiente', RunStaleAttempt: 'Llegar llamada anterior',
  DeleteFirst: 'Aplicar delete de A', PublishNewRow: 'Publicar fila nueva de B',
  Restart: 'Reabrir journal y SOW', ReplayDelete: 'Repetir delete de A', ReplayPublish: 'Repetir publicación de B',
};
export const step = action => names[action] ?? action;
export const title = 'Verificación acotada de recuperación AMPS';
