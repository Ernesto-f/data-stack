import { ExceptionCreate, ExceptionRecord, ExceptionReason } from '../types';

export class ApiClient {
  private baseUrl: string;
  private apiKey:  string;

  constructor(baseUrl: string, apiKey: string) {
    this.baseUrl = baseUrl.trim().replace(/^['\"]|['\"]$/g, '').replace(/\/$/, '');
    this.apiKey  = apiKey;
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'X-Api-Key':    this.apiKey,
        ...options.headers,
      },
    });

    if (!response.ok) {
      const err = await response.json().catch(() => ({ detail: 'Erro desconhecido' }));
      throw { status: response.status, data: err };
    }

    return response.json();
  }

  async getEquipments(): Promise<string[]> {
    return this.request<string[]>('/equipments');
  }

  async getReasons(): Promise<ExceptionReason[]> {
    return this.request<ExceptionReason[]>('/exception-reasons');
  }

  async getExceptions(params: {
    val_equipment?: string;
    dtt_start?:     string;
    dtt_end?:       string;
  }): Promise<ExceptionRecord[]> {
    const qs = new URLSearchParams();
    if (params.val_equipment) qs.set('val_equipment', params.val_equipment);
    if (params.dtt_start)     qs.set('dtt_start',     params.dtt_start);
    if (params.dtt_end)       qs.set('dtt_end',        params.dtt_end);
    return this.request<ExceptionRecord[]>(`/exceptions?${qs.toString()}`);
  }

  async createException(payload: ExceptionCreate): Promise<{ cod_record: number; message: string }> {
    return this.request('/exceptions', {
      method: 'POST',
      body:   JSON.stringify(payload),
    });
  }
}
