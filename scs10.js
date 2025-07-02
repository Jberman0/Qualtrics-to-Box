Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "SCS-10_General_Score", "SCS-10_Covert_Score", "SCS-10_ScoreRaw"
  // AS EMBEDDED DATA BEFORE THIS BLOCK

  // ── Configuration: Item mappings and settings ─────────
  const config = {
    generalRows: [1, 3, 5, 7, 9],  // SCS–G item rows
    covertRows:  [2, 4, 6, 8, 10]  // SCS–C item rows
  };

  // ── Scoring calculation ──────────────────────────────
  let totalGeneralScore = 0;
  let totalCovertScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl, index) => {
    const rowNum = index + 1;
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) return; // Skip if no response selected

    const responseValue = parseInt(selected.value, 10) || 0;

    // Score based on subscale
    if (config.generalRows.includes(rowNum)) {
      totalGeneralScore += responseValue;
    } else if (config.covertRows.includes(rowNum)) {
      totalCovertScore += responseValue;
    }
  });

  const totalScore = totalGeneralScore + totalCovertScore;

  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("SCS-10_General_Score", totalGeneralScore);
  Qualtrics.SurveyEngine.setEmbeddedData("SCS-10_Covert_Score", totalCovertScore);
  Qualtrics.SurveyEngine.setEmbeddedData("SCS-10_ScoreRaw", totalScore);
});