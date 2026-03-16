const pipelineSelect = document.getElementById("pipelineSelect");
const runButton = document.getElementById("runButton");
const editButton = document.getElementById("editButton");
const saveAllButton = document.getElementById("saveAllButton");
const statusEl = document.getElementById("status");
const stepsTableContainer = document.getElementById("stepsTableContainer");

let notebooks = [];
let currentPipelineId = "";
let currentSteps = [];
let isEditMode = false;

function setStatus(message) {
  statusEl.textContent = message;
}

function getPrefix(name) {
  if (!name) return "";
  return String(name).split("_")[0].toLowerCase();
}

function stringifyParams(params) {
  if (!params || Object.keys(params).length === 0) {
    return "<span class='muted'>No parameters</span>";
  }
  return Object.entries(params)
    .map(([key, value]) => `${key}: ${String(value)}`)
    .join("<br/>");
}

function notebooksForStep(step) {
  const prefix = getPrefix(step.notebookName);
  if (!prefix) return notebooks;

  const filtered = notebooks.filter((n) => getPrefix(n.displayName) === prefix);
  if (filtered.length > 0) return filtered;

  return step.notebookName
    ? [{ id: step.notebookId || "", displayName: step.notebookName }]
    : notebooks;
}

function renderViewMode(steps) {
  const headerCells = steps.map((s) => `<th>${s.name || "Unnamed Step"}</th>`).join("");
  const valueCells = steps
    .map(
      (s) => `
      <td>
        <strong>Descrição:</strong> ${s.description || "-"}<br/>
        <strong>Metodologia:</strong> ${s.notebookName || "-"}<br/>
        <strong>Parametros:</strong><br/>
        ${stringifyParams(s.parameters)}
      </td>
    `
    )
    .join("");

  return `
    <table>
      <thead>
        <tr>${headerCells}</tr>
      </thead>
      <tbody>
        <tr>${valueCells}</tr>
      </tbody>
    </table>
  `;
}

function renderEditMode(steps) {
  const headerCells = steps.map((s) => `<th>${s.name || "Unnamed Step"}</th>`).join("");
  const valueCells = steps
    .map((s, idx) => {
      const options = notebooksForStep(s)
        .map((n) => {
          const selected = n.displayName === s.notebookName ? "selected" : "";
          return `<option value="${n.displayName}" ${selected}>${n.displayName}</option>`;
        })
        .join("");

      const paramEntries = Object.entries(s.parameters || {});
      const paramName = paramEntries.length > 0 ? paramEntries[0][0] : "";
      const paramValue = paramEntries.length > 0 ? paramEntries[0][1] : "";

      return `
        <td>
          <strong>Descrição:</strong> ${s.description || "-"}<br/>
          <strong>Metodologia:</strong><br/>
          <select id="notebook-select-${idx}" class="select">
            ${options}
          </select>
          <strong>Parametros:</strong><br/>
          ${
            paramName
              ? `<label class="muted">${paramName}</label>
                 <input id="param-input-${idx}" class="input" value="${String(paramValue)}" />`
              : `<span class="muted">Sem parametro para editar</span>`
          }
        </td>
      `;
    })
    .join("");

  return `
    <table>
      <thead>
        <tr>${headerCells}</tr>
      </thead>
      <tbody>
        <tr>${valueCells}</tr>
      </tbody>
    </table>
  `;
}

function renderSteps(steps) {
  if (!steps.length) {
    stepsTableContainer.innerHTML = "";
    setStatus("No notebook steps found in this pipeline.");
    return;
  }

  stepsTableContainer.innerHTML = isEditMode ? renderEditMode(steps) : renderViewMode(steps);
  setStatus(`Carregadas ${steps.length} etapas`);
}

function updateActionButtons() {
  const hasPipeline = Boolean(currentPipelineId);
  runButton.disabled = !hasPipeline;
  editButton.disabled = !hasPipeline;
}

async function loadPipelines() {
  setStatus("Loading pipelines...");
  try {
    const [pipelinesResp, notebooksResp] = await Promise.all([
      fetch("/pipelines"),
      fetch("/notebooks"),
    ]);
    if (!pipelinesResp.ok) throw new Error(`HTTP ${pipelinesResp.status} in /pipelines`);
    if (!notebooksResp.ok) throw new Error(`HTTP ${notebooksResp.status} in /notebooks`);

    const pipelines = await pipelinesResp.json();
    notebooks = await notebooksResp.json();

    pipelineSelect.innerHTML = '<option value="">Select a pipeline</option>';
    pipelines.forEach((p) => {
      const option = document.createElement("option");
      option.value = p.id;
      option.textContent = p.displayName || p.id;
      pipelineSelect.appendChild(option);
    });

    setStatus("Select a pipeline to load steps.");
    updateActionButtons();
  } catch (error) {
    setStatus(`Error loading pipelines: ${error.message}`);
  }
}

async function loadPipelineSteps(pipelineId) {
  currentPipelineId = pipelineId;
  if (!pipelineId) {
    currentSteps = [];
    stepsTableContainer.innerHTML = "";
    setStatus("Select a pipeline to load steps.");
    updateActionButtons();
    return;
  }

  setStatus("Loading pipeline steps...");
  try {
    const response = await fetch(`/pipelines/${pipelineId}/steps`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    currentSteps = await response.json();
    renderSteps(currentSteps);
    updateActionButtons();
  } catch (error) {
    currentSteps = [];
    stepsTableContainer.innerHTML = "";
    setStatus(`Error loading steps: ${error.message}`);
    updateActionButtons();
  }
}

async function saveAllSteps() {
  if (!currentPipelineId || !currentSteps.length) return;

  try {
    setStatus("Salvando todas as etapas...");
    for (let index = 0; index < currentSteps.length; index += 1) {
      const step = currentSteps[index];
      const notebookSelect = document.getElementById(`notebook-select-${index}`);
      const selectedNotebook = notebookSelect ? notebookSelect.value : step.notebookName;
      const paramEntries = Object.entries(step.parameters || {});
      const hasParam = paramEntries.length > 0;
      const paramName = hasParam ? paramEntries[0][0] : null;
      const paramInput = document.getElementById(`param-input-${index}`);
      const paramValue = hasParam && paramInput ? paramInput.value : null;

      const body = {
        step: step.name,
        notebook_name: selectedNotebook,
      };
      if (paramName) {
        body.base_parameter_name = paramName;
        body.base_parameter_value = paramValue;
      }

      const response = await fetch(`/pipelines/${currentPipelineId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok) {
        const err = await response.text();
        throw new Error(`Erro na etapa ${step.name}: HTTP ${response.status} ${err}`);
      }
    }

    setStatus("Todas as etapas foram salvas com sucesso.");
    isEditMode = false;
    editButton.textContent = "Editar";
    saveAllButton.style.display = "none";
    runButton.style.display = "inline-block";
    await loadPipelineSteps(currentPipelineId);
  } catch (error) {
    setStatus(`Erro ao salvar: ${error.message}`);
  }
}

async function runCurrentPipeline() {
  if (!currentPipelineId) return;
  try {
    setStatus("Disparando execução do pipeline...");
    const response = await fetch(`/pipelines/${currentPipelineId}/run`, {
      method: "POST",
    });
    if (!response.ok) {
      const err = await response.text();
      throw new Error(`HTTP ${response.status}: ${err}`);
    }
    const result = await response.json();
    setStatus(`Pipeline disparado com sucesso (status ${result.statusCode}).`);
  } catch (error) {
    setStatus(`Erro ao executar pipeline: ${error.message}`);
  }
}

pipelineSelect.addEventListener("change", (event) => {
  loadPipelineSteps(event.target.value);
});

editButton.addEventListener("click", () => {
  isEditMode = !isEditMode;
  editButton.textContent = isEditMode ? "Cancelar" : "Editar";
  saveAllButton.style.display = isEditMode ? "inline-block" : "none";
  runButton.style.display = isEditMode ? "none" : "inline-block";
  renderSteps(currentSteps);
});

saveAllButton.addEventListener("click", saveAllSteps);
runButton.addEventListener("click", runCurrentPipeline);

loadPipelines();
