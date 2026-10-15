const API_BASE = "/fileupload/api";

const routeInput = document.getElementById("route_key");
const fileInput  = document.getElementById("file");
const btnUpload  = document.getElementById("btnUpload");
const btnClear   = document.getElementById("btnClear");

const statusText  = document.getElementById("statusText");
const statusDot   = document.getElementById("statusDot");
const responseBox = document.getElementById("response");

let pollTimer = null;

// ---------------- UI helpers ----------------
function setStatus(text, cls = "idle") {
  statusText.innerText = text;
  statusDot.className = "dot " + cls;
}

function setBusy(isBusy) {
  btnUpload.disabled = isBusy;
  btnClear.disabled = isBusy;
  btnUpload.style.opacity = isBusy ? "0.7" : "1";
  btnClear.style.opacity  = isBusy ? "0.7" : "1";
}

function showResponse(objOrText) {
  responseBox.style.display = "block";
  responseBox.textContent =
    typeof objOrText === "string"
      ? objOrText
      : JSON.stringify(objOrText, null, 2);
}

function hideResponse() {
  responseBox.style.display = "none";
  responseBox.textContent = "";
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

function clearForm() {
  routeInput.value = "";
  fileInput.value = "";
  hideResponse();
  setStatus("Aguardando...", "idle");
  stopPolling();
  setBusy(false);
}

async function safeReadJson(res) {
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (ct.includes("application/json")) return await res.json();
  const txt = await res.text();
  try { return JSON.parse(txt); } catch { return txt; }
}

function validateFile(file) {
  const name = (file?.name || "").toLowerCase();
  const isCsv  = name.endsWith(".csv");
  const isXlsx = name.endsWith(".xlsx");
  const isXlsm = name.endsWith(".xlsm");

  if (!isCsv && !isXlsx && !isXlsm) return "Formato inválido. Selecione .xlsx ou .csv.";

  const MAX_MB = 200;
  const sizeMb = file.size / (1024 * 1024);
  if (sizeMb > MAX_MB) {
    return `Arquivo muito grande (${sizeMb.toFixed(1)} MB). Limite: ${MAX_MB} MB. Prefira CSV ou divida o arquivo.`;
  }
  return null;
}

// ---------------- API fallbacks ----------------
async function postUpload(formData) {
  let res = await fetch(`${API_BASE}`, { method: "POST", body: formData });
  if (res.status !== 404) return res;
  return await fetch(`${API_BASE}/upload/`, { method: "POST", body: formData });
}

async function getStatus(uploadId) {
  let res = await fetch(`${API_BASE}/status/${encodeURIComponent(uploadId)}`, { cache: "no-store" });
  if (res.status !== 404) return res;
  return await fetch(`${API_BASE}/upload/status/${encodeURIComponent(uploadId)}`, { cache: "no-store" });
}

// ---------------- Polling ----------------
async function pollStatus(uploadId) {
  stopPolling();

  pollTimer = setInterval(async () => {
    try {
      const res = await getStatus(uploadId);
      const data = await safeReadJson(res);

      if (!res.ok) return;
      if (!data || typeof data !== "object" || !data.status) return;

      if (data.status === "UPLOADED")   setStatus("Upload recebido...", "busy");
      if (data.status === "PROCESSING") setStatus("Processando...", "busy");

      if (data.status === "DONE") {
        setStatus("Concluído com sucesso", "ok");
        stopPolling();
        setBusy(false);
      }

      if (data.status === "FAILED") {
        setStatus("Falha no processamento", "err");
        stopPolling();
        setBusy(false);
        alert(data.error || "Erro desconhecido");
      }
    } catch {
      // ignora falhas momentâneas
    }
  }, 2000);
}

// ---------------- Upload ----------------
btnUpload.addEventListener("click", async () => {
  const routeKey = (routeInput.value || "").trim().toUpperCase();
  const file = fileInput.files?.[0];

  if (!routeKey) {
    alert("Cole a route_key (chave) antes de enviar.");
    routeInput.focus();
    return;
  }

  if (!file) {
    alert("Selecione um arquivo (.xlsx, .csv, .xlsm).");
    fileInput.click();
    return;
  }

  const fileErr = validateFile(file);
  if (fileErr) {
    alert(fileErr);
    return;
  }

  const fd = new FormData();
  fd.append("route_key", routeKey);
  fd.append("file", file);

  setBusy(true);
  setStatus("Enviando...", "busy");
  hideResponse();
  stopPolling();

  try {
    const res = await postUpload(fd);
    const data = await safeReadJson(res);

    if (!res.ok) {
      setStatus("Erro no upload", "err");
      setBusy(false);

      const msg =
        (data && typeof data === "object" && (data.detail || data.error)) ||
        (typeof data === "string" ? data : null) ||
        "Erro no upload";

      showResponse(data);
      alert(msg);
      return;
    }

    showResponse(data);

    if (data && typeof data === "object" && data.upload_id) {
      setStatus("Upload aceito. Aguardando processamento...", "busy");
      pollStatus(data.upload_id);
    } else {
      setStatus("Upload aceito, mas resposta sem upload_id", "busy");
      setBusy(false);
    }
  } catch (e) {
    setStatus("Falha de rede / API indisponível", "err");
    setBusy(false);
    showResponse(String(e));
    alert("Falha de rede / API indisponível. Veja o console (F12).");
  }
});

btnClear.addEventListener("click", clearForm);

// estado inicial
clearForm();
