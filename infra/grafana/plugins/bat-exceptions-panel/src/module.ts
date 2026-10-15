import { PanelPlugin } from '@grafana/data';
import { ExceptionPanel } from './components/ExceptionPanel';
import { PluginOptions } from './types';

export const plugin = new PanelPlugin<PluginOptions>(ExceptionPanel).setPanelOptions(builder => {
  builder
    .addTextInput({
      path:         'apiUrl',
      name:         'URL da API',
      description:  'Endereço da exceptions-api (ex: http://exceptions-api:8080)',
      defaultValue: 'http://localhost:8080',
    })
    .addTextInput({
      path:        'apiKey',
      name:        'API Key',
      description: 'Token de autenticação configurado no servidor',
      defaultValue: '',
      settings:    { placeholder: 'Token secreto...' },
    })
    .addBooleanSwitch({
      path:         'showList',
      name:         'Mostrar lista de excepções recentes',
      defaultValue: true,
    })
    .addNumberInput({
      path:         'listDays',
      name:         'Dias a exibir na lista',
      defaultValue: 7,
      settings:     { min: 1, max: 90 },
    });
});
