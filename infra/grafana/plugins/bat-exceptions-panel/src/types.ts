export interface PluginOptions {
  apiUrl:   string;
  apiKey:   string;
  showList: boolean;
  listDays: number;
}

export interface ExceptionReason {
  cod_record:           number;
  nam_exception_reason: string;
  val_external_code:    string | null;
}

export interface ExceptionRecord {
  cod_record:           number;
  val_equipment:        string;
  val_workcenter:       string | null;
  dtt_start:            string;
  dtt_end:              string;
  hrr_start:            string | null;
  hrr_end:              string | null;
  cod_shift:            string | null;
  nam_exception_reason: string | null;
  des_comment:          string | null;
  usr_grafana:          string | null;
  dtt_record:           string;
}

export interface ExceptionCreate {
  val_equipment:        string;
  val_workcenter:       string | null;
  val_department:       string | null;
  dtt_start:            string;
  dtt_end:              string;
  cod_exception_reason: number;
  des_comment:          string;
  usr_grafana:          string;
}

export interface ApiError {
  detail: string | { message: string; existing?: ExceptionRecord };
}
