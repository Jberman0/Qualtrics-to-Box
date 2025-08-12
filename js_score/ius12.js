Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD "IUS-12_ScoreRaw" AS EMBEDDED DATA BEFORE THIS BLOCK

  // ── Scoring calculation ──────────────────────────────
  let totalScore = 0;
  let itemCount = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl, index) => {
    const rowNum = index + 1;
    const selected = rowEl.querySelector('input[type="radio"]:checked');

    if (!selected) {
      return;
    }

    const responseValue = parseInt(selected.value, 10);
    totalScore += responseValue || 0;
    itemCount++;
  });

  // ── Set embedded data ────────────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("IUS-12_ScoreRaw", totalScore);

  // Save display order to participant
  const currentOrder = Qualtrics.SurveyEngine.getEmbeddedData("displayOrder") || "";
  if (!currentOrder.split(", ").includes("IUS-12")) {
    let updatedOrder = currentOrder ? currentOrder + ", " + "IUS-12" : "IUS-12";
    Qualtrics.SurveyEngine.setEmbeddedData("displayOrder", updatedOrder);
  }
});