const url = 'http://10.113.120.120/exceptions-api/';

const container = context.panel.chart.getDom();

container.innerHTML = '';

const iframe = document.createElement('iframe');

iframe.src = url;
iframe.style.width = '100%';
iframe.style.height = '100%';
iframe.style.minHeight = '100%';
iframe.style.border = '0';
iframe.style.margin = '0';
iframe.style.padding = '0';
iframe.style.display = 'block';
iframe.style.background = '#111217';

container.style.width = '100%';
container.style.height = '100%';
container.style.margin = '0';
container.style.padding = '0';
container.style.overflow = 'hidden';

container.appendChild(iframe);

return {};