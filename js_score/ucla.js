Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "UCLA_Loneliness_ScoreRaw" AS EMBEDDED DATA BEFORE THIS BLOCK
  
  // ── Scoring calculation ─────────────────────────
  let totalScore = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl) => {
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) return; // Skip if no response selected

    const responseValue =  (4 - parseInt(selected.value, 10)) || 0;
    totalScore += responseValue;
  });

  // ── Set embedded data ──────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("UCLA_Loneliness_ScoreRaw", totalScore);

  // Save display order to participant
  const currentOrder = Qualtrics.SurveyEngine.getEmbeddedData("displayOrder") || "";
  if (!currentOrder.split(", ").includes("UCLA Loneliness")) {
    let updatedOrder = currentOrder ? currentOrder + ", " + "UCLA Loneliness" : "UCLA Loneliness";
    Qualtrics.SurveyEngine.setEmbeddedData("displayOrder", updatedOrder);
  }
});