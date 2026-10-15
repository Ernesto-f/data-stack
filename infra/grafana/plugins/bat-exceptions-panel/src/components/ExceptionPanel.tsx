import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { PanelProps } from '@grafana/data';
import { PluginOptions, ExceptionReason, ExceptionRecord, ExceptionCreate } from '../types';
import { ApiClient } from '../api/client';
import { ExceptionModal } from './ExceptionModal';
import { ExceptionList } from './ExceptionList';

interface Props extends PanelProps<PluginOptions> {}

export const ExceptionPanel: React.FC<Props> = ({ options, timeRange, replaceVariables }) => {
  const client = new ApiClient(options.apiUrl, options.apiKey);

  const equipmentVarCsv = replaceVariables('${equipamento:csv}');
  const equipmentVarRaw = replaceVariables('${equipamento}');
  const equipmentFromVariable = useMemo(() => {
    const normalized = (equipmentVarCsv || equipmentVarRaw || '').trim();
    if (!normalized || normalized.includes('${equipamento')) {
      return [] as string[];
    }

    const values = normalized
      .split(',')
      .map((v) => v.trim().replace(/^['\"]|['\"]$/g, ''))
      .filter(Boolean);

    if (values.length === 0) {
      return [] as string[];
    }

    const lower = values.map((v) => v.toLowerCase());
    if (lower.includes('all') || lower.includes('$__all') || lower.includes('*')) {
      return [] as string[];
    }

    return Array.from(new Set(values));
  }, [equipmentVarCsv, equipmentVarRaw]);

  // Equipamentos disponíveis — carrega da API
  const [equipments, setEquipments] = useState<string[]>([]);

  const [showModal,    setShowModal]    = useState(false);
  const [reasons,      setReasons]      = useState<ExceptionReason[]>([]);
  const [records,      setRecords]      = useState<ExceptionRecord[]>([]);
  const [listLoading,  setListLoading]  = useState(false);
  const [modalLoading, setModalLoading] = useState(false);
  const [modalError,   setModalError]   = useState<string | null>(null);
  const [modalSuccess, setModalSuccess] = useState(false);

  // Carrega equipamentos e motivos uma vez
  useEffect(() => {
    if (equipmentFromVariable.length > 0) {
      setEquipments(equipmentFromVariable);
    } else {
      client.getEquipments().then(setEquipments).catch(() => {});
    }
    client.getReasons().then(setReasons).catch(() => {});
  }, [options.apiUrl, options.apiKey, equipmentFromVariable]);

  // Carrega lista de excepções
  const loadList = useCallback(() => {
    if (!options.showList) return;
    setListLoading(true);
    const dttStart = new Date(timeRange.from.valueOf());
    dttStart.setDate(dttStart.getDate() - options.listDays);
    const equipmentFilter = equipmentFromVariable.length === 1 ? equipmentFromVariable[0] : undefined;
    client
      .getExceptions({
        val_equipment: equipmentFilter,
        dtt_start: dttStart.toISOString(),
        dtt_end:   timeRange.to.toISOString(),
      })
      .then(setRecords)
      .catch(() => {})
      .finally(() => setListLoading(false));
  }, [options, timeRange, equipmentFromVariable]);

  useEffect(() => { loadList(); }, [loadList]);

  const handleSubmit = async (payload: ExceptionCreate) => {
    setModalLoading(true);
    setModalError(null);
    try {
      await client.createException(payload);
      setModalSuccess(true);
      loadList();
      setTimeout(() => {
        setShowModal(false);
        setModalSuccess(false);
      }, 1500);
    } catch (err: any) {
      const detail = err?.data?.detail;
      if (typeof detail === 'object' && detail?.message) {
        setModalError(detail.message);
      } else {
        setModalError(typeof detail === 'string' ? detail : 'Erro ao registrar excepção');
      }
    } finally {
      setModalLoading(false);
    }
  };

  const timeFrom = new Date(timeRange.from.valueOf()).toISOString().slice(0, 16);
  const timeTo   = new Date(timeRange.to.valueOf()).toISOString().slice(0, 16);

  return (
    <div style={styles.container}>
      {/* Header */}
      <div style={styles.header}>
        <span style={styles.title}>⚡ Excepções de Parada</span>
        <button
          style={styles.openBtn}
          onClick={() => {
            setModalError(null);
            setModalSuccess(false);
            setShowModal(true);
          }}
        >
          + Registrar Excepção
        </button>
      </div>

      {/* Lista */}
      {options.showList && (
        <ExceptionList records={records} isLoading={listLoading} />
      )}

      {/* Modal */}
      {showModal && (
        <ExceptionModal
          equipment={equipments[0]}
          equipments={equipments}
          timeFrom={timeFrom}
          timeTo={timeTo}
          reasons={reasons}
          onSubmit={handleSubmit}
          onClose={() => setShowModal(false)}
          isLoading={modalLoading}
          error={modalError}
          success={modalSuccess}
        />
      )}
    </div>
  );
};

const styles: Record<string, React.CSSProperties> = {
  container: {
    padding:       12,
    height:        '100%',
    display:       'flex',
    flexDirection: 'column',
    gap:           12,
    overflow:      'hidden',
  },
  header: {
    display:        'flex',
    justifyContent: 'space-between',
    alignItems:     'center',
  },
  title: {
    color:      '#e0e0e0',
    fontWeight: 700,
    fontSize:   14,
  },
  openBtn: {
    backgroundColor: '#2e75b6',
    border:          'none',
    borderRadius:    4,
    color:           '#fff',
    padding:         '7px 14px',
    fontSize:        13,
    fontWeight:      600,
    cursor:          'pointer',
  },
};
