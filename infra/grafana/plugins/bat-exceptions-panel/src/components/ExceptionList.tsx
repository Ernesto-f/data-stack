import React from 'react';
import { ExceptionRecord } from '../types';

interface Props {
  records:   ExceptionRecord[];
  isLoading: boolean;
}

const fmt = (iso: string) =>
  new Date(iso).toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });

export const ExceptionList: React.FC<Props> = ({ records, isLoading }) => {
  if (isLoading) {
    return <div style={styles.empty}>Carregando...</div>;
  }

  if (records.length === 0) {
    return <div style={styles.empty}>Nenhuma excepção registrada no período.</div>;
  }

  return (
    <div style={styles.list}>
      {records.map(r => (
        <div key={r.cod_record} style={styles.item}>
          <div style={styles.itemHeader}>
            <span style={styles.equipment}>{r.val_equipment}</span>
            <span style={styles.reason}>{r.nam_exception_reason ?? '—'}</span>
          </div>
          <div style={styles.itemDates}>
            {fmt(r.dtt_start)} → {fmt(r.dtt_end)}
          </div>
          {r.des_comment && (
            <div style={styles.comment}>{r.des_comment}</div>
          )}
        </div>
      ))}
    </div>
  );
};

const styles: Record<string, React.CSSProperties> = {
  list: {
    display:       'flex',
    flexDirection: 'column',
    gap:           6,
    maxHeight:     320,
    overflowY:     'auto',
  },
  empty: {
    color:    '#666',
    fontSize: 13,
    padding:  '12px 0',
  },
  item: {
    backgroundColor: '#1e2128',
    border:          '1px solid #2a2d33',
    borderRadius:    5,
    padding:         '10px 12px',
  },
  itemHeader: {
    display:        'flex',
    justifyContent: 'space-between',
    alignItems:     'center',
    marginBottom:   4,
  },
  equipment: {
    color:      '#e0e0e0',
    fontWeight: 700,
    fontSize:   13,
  },
  reason: {
    color:        '#2e75b6',
    fontSize:     12,
    background:   'rgba(46,117,182,0.12)',
    borderRadius: 3,
    padding:      '2px 8px',
  },
  itemDates: {
    color:    '#888',
    fontSize: 12,
  },
  comment: {
    color:     '#aaa',
    fontSize:  12,
    marginTop: 4,
    fontStyle: 'italic',
  },
};
