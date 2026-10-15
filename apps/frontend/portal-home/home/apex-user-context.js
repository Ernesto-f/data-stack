(function () {
  'use strict';

  const API_URL = '/oauth2/userinfo';

  function normalizeGroup(value) {
    return String(value || '')
      .trim()
      .replace(/^\/+/, '')
      .replace(/\/+$/, '')
      .toLowerCase();
  }

  function normalizeGroups(groups) {
    if (!Array.isArray(groups)) {
      return [];
    }

    const uniqueGroups = new Set();

    for (const group of groups) {
      const normalized = normalizeGroup(group);

      if (normalized) {
        uniqueGroups.add(normalized);
      }
    }

    return [...uniqueGroups];
  }

  async function load() {
    const response = await fetch(API_URL, {
      method: 'GET',
      credentials: 'same-origin',
      headers: {
        Accept: 'application/json'
      },
      cache: 'no-store'
    });

    if (!response.ok) {
      throw new Error(
        `Erro ao carregar contexto do usuário: HTTP ${response.status}`
      );
    }

    const data = await response.json();

    const email = String(data.email || '');

    const username =
      email.includes('@')
        ? email.split('@')[0]
        : String(data.user || '');

    return {
      username,
      email,
      groups: normalizeGroups(data.groups)
    };
  }

  window.PulseUserContext = {
    load,
    normalizeGroup
  };
})();
