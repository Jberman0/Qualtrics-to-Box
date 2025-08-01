Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD THESE AS EMBEDDED DATA BEFORE THIS BLOCK:
  // "LSAS-SR_AnxiousRaw", "LSAS-SR_AvoidanceRaw", "LSAS-SR_ScoreRaw", "LSAS-SR_ScoreSummary"
  
  // ── Configuration: Scoring settings ─────────────────
  const config = {
    severityLevels: [
      { max: 29, label: 'No social anxiety' },
      { max: 49, label: 'Mild social anxiety' },
      { max: 64, label: 'Moderate social anxiety' },
      { max: 79, label: 'Marked social anxiety' },
      { max: 94, label: 'Severe social anxiety' },
      { max: Infinity, label: 'Very severe social anxiety' }
    ],
    anxiousRows: [],
    avoidanceRows: []
  };

  // ── Define row mappings ─────────────────────────────
  for (let i = 1; i <= 48; i++) {
    if ((i >= 1 && i <= 24)) {
      config.anxiousRows.push(i);
    } else {
      config.avoidanceRows.push(i);
    }
  }

  // ── Scoring calculation ──────────────────────────────
  let AnxiousRaw = 0;
  let AvoidanceRaw = 0;
  let totalScore = 0;
  let itemCount = 0;
  const rows = document.querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl, index) => {
    const rowNum = index + 1;
    const selected = rowEl.querySelector('input[type="radio"]:checked');

    if (!selected) return;

    const responseValue = parseInt(selected.value, 10) - 1;
    totalScore += responseValue || 0;
    itemCount++;

    if (config.anxiousRows.includes(rowNum)) {
      AnxiousRaw += responseValue;
    }

    if (config.avoidanceRows.includes(rowNum)) {
      AvoidanceRaw += responseValue;
    }
  });

  // Determine severity level
  const scoreSummary = config.severityLevels.find(level => totalScore <= level.max).label;

  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("LSAS-SR_AnxiousRaw", AnxiousRaw);
  Qualtrics.SurveyEngine.setEmbeddedData("LSAS-SR_AvoidanceRaw", AvoidanceRaw);
  Qualtrics.SurveyEngine.setEmbeddedData("LSAS-SR_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("LSAS-SR_ScoreSummary", scoreSummary);

  // Save display order to participant
  const currentOrder = Qualtrics.SurveyEngine.getEmbeddedData("displayOrder");
  let updatedOrder = currentOrder ? currentOrder + ", " + "LSAS-SR" : "LSAS-SR";
  Qualtrics.SurveyEngine.setEmbeddedData("displayOrder", updatedOrder);
});