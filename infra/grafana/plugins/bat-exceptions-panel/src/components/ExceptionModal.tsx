import React, { useState, useEffect, useMemo } from 'react';
import { Modal } from '@grafana/ui';
import { ExceptionReason, ExceptionCreate } from '../types';

interface Props {
  equipment:    string;
  equipments:   string[];
  timeFrom:     string;
  timeTo:       string;
  reasons:      ExceptionReason[];
  onSubmit:     (payload: ExceptionCreate) => Promise<void>;
  onClose:      () => void;
  isLoading:    boolean;
  error:        string | null;
  success:      boolean;
}

export const ExceptionModal: React.FC<Props> = ({
  equipment, equipments, timeFrom, timeTo,
  reasons, onSubmit, onClose, isLoading, error, success,
}) => {
  const equipmentOptions = useMemo(
    () => equipments.map(eq => ({ label: eq, value: eq })),
    [equipments]
  );

  const reasonOptions = useMemo(
    () => reasons.map(r => ({ label: r.nam_exception_reason, value: r.cod_record })),
    [reasons]
  );

  const [form, setForm] = useState({
    val_equipment:        equipment || (equipments[0] ?? ''),
    dtt_start:            timeFrom,
    dtt_end:              timeTo,
    cod_exception_reason: reasons[0]?.cod_record ?? 0,
    des_comment:          '',
  });

  useEffect(() => {
    if (equipments.length === 0) {
      return;
    }

    const current = form.val_equipment?.trim();
    const hasCurrent = current ? equipments.includes(current) : false;
    if (!hasCurrent) {
      setForm(f => ({ ...f, val_equipment: equipments[0] }));
    }
  }, [equipments, form.val_equipment]);

  useEffect(() => {
    if (reasons.length > 0 && form.cod_exception_reason === 0) {
      setForm(f => ({ ...f, cod_exception_reason: reasons[0].cod_record }));
    }
    if (reasons.length > 0 && !reasons.some(r => r.cod_record === form.cod_exception_reason)) {
      setForm(f => ({ ...f, cod_exception_reason: reasons[0].cod_record }));
    }
  }, [reasons, form.cod_exception_reason]);

  const handleSubmit = () => {
    onSubmit({
      val_equipment:        form.val_equipment,
      val_workcenter:       null,
      val_department:       null,
      dtt_start:            form.dtt_start,
      dtt_end:              form.dtt_end,
      cod_exception_reason: form.cod_exception_reason,
      des_comment:          form.des_comment,
      usr_grafana:          'grafana',
    });
  };

  const modal = (
    <Modal title="Registrar Excepção" isOpen={true} onDismiss={onClose}>
      <div className="bat-exception-modal">
        <style>{`
          .bat-exception-modal .bat-native-select {
            width: 100%;
            min-height: 40px;
            background: #111217 !important;
            border: 1px solid #333 !important;
            border-radius: 4px;
            padding: 10px 12px;
            box-sizing: border-box;
            color: #e8edf2 !important;
            -webkit-text-fill-color: #e8edf2 !important;
            appearance: auto;
          }
          .bat-exception-modal .bat-native-select option {
            color: #e8edf2 !important;
            background: #151a21 !important;
          }
          .bat-exception-modal .bat-selected-chip {
            margin-top: 6px;
            color: #a8b7c7;
            font-size: 12px;
          }
        `}</style>

        <div style={styles.body}>

          {/* Equipamento */}
          <label style={styles.label}>Equipamento *</label>
          <select
            className="bat-native-select"
            value={form.val_equipment}
            onChange={e => setForm(f => ({ ...f, val_equipment: e.target.value }))}
          >
            <option value="" disabled>Selecione um equipamento</option>
            {equipmentOptions.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
          <div className="bat-selected-chip">Selecionado: {form.val_equipment || '---'}</div>

          {/* Datas */}
          <div style={styles.row}>
            <div style={styles.col}>
              <label style={styles.label}>Data início *</label>
              <input
                type="datetime-local"
                style={styles.input}
                value={form.dtt_start.slice(0, 16)}
                onChange={e => setForm(f => ({ ...f, dtt_start: e.target.value + ':00' }))}
              />
            </div>
            <div style={styles.col}>
              <label style={styles.label}>Data fim *</label>
              <input
                type="datetime-local"
                style={styles.input}
                value={form.dtt_end.slice(0, 16)}
                onChange={e => setForm(f => ({ ...f, dtt_end: e.target.value + ':00' }))}
              />
            </div>
          </div>

          {/* Motivo */}
          <label style={styles.label}>Motivo *</label>
          <select
            className="bat-native-select"
            value={String(form.cod_exception_reason)}
            onChange={e => setForm(f => ({ ...f, cod_exception_reason: Number(e.target.value) }))}
          >
            <option value="0" disabled>Selecione um motivo</option>
            {reasonOptions.map(o => <option key={String(o.value)} value={String(o.value)}>{o.label}</option>)}
          </select>
          <div className="bat-selected-chip">
            Selecionado: {reasonOptions.find(o => o.value === form.cod_exception_reason)?.label || '---'}
          </div>

          {/* Comentário */}
          <label style={styles.label}>Comentário</label>
          <textarea
            style={{ ...styles.input, height: 72, resize: 'vertical' }}
            value={form.des_comment}
            onChange={e => setForm(f => ({ ...f, des_comment: e.target.value }))}
            placeholder="Opcional..."
          />

          {/* Erro */}
          {error && (
            <div style={styles.errorBox}>
              {error}
            </div>
          )}

          {/* Sucesso */}
          {success && (
            <div style={styles.successBox}>
              Excepção registrada com sucesso
            </div>
          )}
        </div>

        <div style={styles.footer}>
          <button style={styles.cancelBtn} onClick={onClose} disabled={isLoading}>
            Cancelar
          </button>
          <button style={styles.confirmBtn} onClick={handleSubmit} disabled={isLoading || success}>
            {isLoading ? 'Gravando...' : 'Confirmar'}
          </button>
        </div>
      </div>
    </Modal>
  );

  return modal;
};

const styles: Record<string, React.CSSProperties> = {
  body: {
    padding:       '20px 22px',
    display:       'flex',
    flexDirection: 'column',
    gap:           10,
    overflowY:     'auto',
  },
  label: {
    color:    '#aaa',
    fontSize: 12,
    fontWeight: 600,
    margin:   0,
  },
  input: {
    width:           '100%',
    backgroundColor: '#111217',
    border:          '1px solid #333',
    borderRadius:    4,
    color:           '#e0e0e0',
    padding:         '10px 12px',
    minHeight:       40,
    fontSize:        14,
    WebkitTextFillColor: '#e0e0e0',
    boxSizing:       'border-box',
  },
  row: {
    display:   'flex',
    flexWrap:  'wrap',
    gap:       12,
    width:     '100%',
  },
  col: {
    flex:          1,
    minWidth:      250,
    display:       'flex',
    flexDirection: 'column',
    gap:           4,
  },
  errorBox: {
    backgroundColor: '#3a1a1a',
    border:          '1px solid #c0392b',
    borderRadius:    4,
    color:           '#e74c3c',
    padding:         '10px 12px',
    fontSize:        13,
  },
  successBox: {
    backgroundColor: '#1a3a1a',
    border:          '1px solid #27ae60',
    borderRadius:    4,
    color:           '#2ecc71',
    padding:         '10px 12px',
    fontSize:        13,
  },
  footer: {
    display:        'flex',
    justifyContent: 'flex-end',
    gap:            10,
    padding:        '14px 20px',
    borderTop:      '1px solid #2a2d33',
  },
  cancelBtn: {
    backgroundColor: 'transparent',
    border:          '1px solid #444',
    borderRadius:    4,
    color:           '#aaa',
    padding:         '8px 18px',
    fontSize:        13,
    cursor:          'pointer',
  },
  confirmBtn: {
    backgroundColor: '#2e75b6',
    border:          'none',
    borderRadius:    4,
    color:           '#fff',
    padding:         '8px 18px',
    fontSize:        13,
    fontWeight:      600,
    cursor:          'pointer',
  },
};
