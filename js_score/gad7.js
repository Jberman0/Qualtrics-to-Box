Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "GAD-7_ScoreRaw" & "GAD-7_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK
  
  // ── Configuration: Scoring settings ─────────────────
  const severityLevels = [
      { max: 4, label: 'Minimal Anxiety' },
      { max: 9, label: 'Mild Anxiety' },
      { max: 14, label: 'Moderate Anxiety' },
      { max: Infinity, label: 'Severe Anxiety' }
    ];
  
  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');
  
  rows.forEach(rowEl => {
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) return;
    
    const responseValue = (parseInt(selected.value, 10) - 1) || 0; // Convert to 0-based index (0-3)
    totalScore += responseValue;
  });
  
  // Determine severity level
  const scoreSummary = severityLevels.find(level => totalScore <= level.max).label;
  
  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("GAD-7_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("GAD-7_ScoreSummary", scoreSummary);
});