Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "PHQ-9_ScoreRaw" & "PHQ-9_ScoreSummary" AS EMBEDDED DATA BEFORE THIS BLOCK

  // ── Configuration: Scoring settings ─────────────────
  const severityLevels = [
      { max: 4, label: 'None-minimal' },
      { max: 9, label: 'Mild' },
      { max: 14, label: 'Moderate' },
      { max: 19, label: 'Moderately Severe' },
      { max: Infinity, label: 'Severe' }
    ];

  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl, index) => {
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) {
      return;
    }

    const responseValue = parseInt(selected.value, 10) - 1;
    totalScore += responseValue || 0;
  });

  // Determine severity level
  const scoreSummary = severityLevels.find(level => totalScore <= level.max).label;

  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("PHQ-9_ScoreRaw", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("PHQ-9_ScoreSummary", scoreSummary);
});