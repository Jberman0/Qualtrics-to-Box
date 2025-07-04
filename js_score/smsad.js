Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "SMSAD_ScoreRaw", "SMSAD_ScoreAverage", & "SMSAD_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK
  
  // ── Configuration: Scoring settings ─────────────────
  const severityLevels = [
      { value: 0, label: 'None' },
      { value: 1, label: 'Mild' },
      { value: 2, label: 'Moderate' },
      { value: 3, label: 'Severe' },
      { value: 4, label: 'Extreme' }
    ];
  
  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');
  
  rows.forEach(rowEl => {
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) return;
    
    const responseValue = (parseInt(selected.value, 10) - 1) || 0; // Convert to 0-based index (0-4)
    totalScore += responseValue;
  });
  
  // Calculate average score
  const averageScore = Math.round(totalScore / 10);
  
  // Determine severity level
  const severityLevel = severityLevels.find(level => level.value === averageScore);
  const scoreSummary = severityLevel ? severityLevel.label : 'Extreme';
  
  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("SMSAD_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("SMSAD_ScoreAverage", averageScore);
  Qualtrics.SurveyEngine.setEmbeddedData("SMSAD_ScoreSummary", scoreSummary);
});