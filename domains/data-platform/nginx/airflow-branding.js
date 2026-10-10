(() => {
  const productName = 'Pulse Automation'

  function applyBranding() {
    if (document.title === 'Airflow') {
      document.title = productName
    }

    document.querySelectorAll('span, a, button, [aria-label], [title]').forEach((element) => {
      if (element.children.length === 0 && ['Airflow', 'Welcome to Airflow'].includes(element.textContent.trim())) {
        element.textContent = productName
      }
      if (['Airflow', 'Welcome to Airflow'].includes(element.getAttribute('aria-label'))) {
        element.setAttribute('aria-label', productName)
      }
      if (['Airflow', 'Welcome to Airflow'].includes(element.getAttribute('title'))) {
        element.setAttribute('title', productName)
      }
    })

    document.querySelectorAll('svg').forEach((icon) => {
      const mark = icon.querySelector('path')?.getAttribute('d') || ''
      if (!mark.startsWith('M0.861099')) {
        return
      }

      const logo = document.createElement('img')
      logo.src = '/pulse-logo.svg'
      logo.alt = productName
      logo.style.height = '32px'
      logo.style.width = '140px'
      logo.style.objectFit = 'contain'
      icon.replaceWith(logo)
    })
  }

  applyBranding()
  new MutationObserver(applyBranding).observe(document.documentElement, {
    childList: true,
    subtree: true,
  })
})()
