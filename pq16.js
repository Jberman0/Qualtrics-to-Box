Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD THESE AS EMBEDDED DATA BEFORE THIS BLOCK:
  // "PQ-16_DistressScore", "PQ-16_DistressSummary", "PQ-16_SymptomScore", "PQ-16_SymptomSummary"

  // ── Configuration: Scoring settings ─────────────
  const thresholds = {
      total: 9,     // Threshold for Distress Score
      symptom: 6    // Threshold for Symptom Count
    };

  // ── Scoring calculation ─────────────────────────
  let totalScore = 0;
  let symptomCount = 0;
  const rows = this.getQuestionContainer().querySelectorAll('.ChoiceRow');

  rows.forEach((rowEl) => {
    const selected = rowEl.querySelector('input[type="radio"]:checked');
    if (!selected) return; // Skip if no response selected

    const distressValue = (parseInt(selected.value, 10) - 1) || 0;
    const score = distressValue;
    totalScore += score;

    if (distressValue !== 0) { // Not "None"
      symptomCount++;
    }
  });

  // Determine summaries
  const scoreSummary = totalScore >= thresholds.total ? "Significant" : "Not Significant";
  const symptomSummary = symptomCount >= thresholds.symptom ? "Significant" : "Not Significant";

  // ── Set embedded data ──────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_DistressScore", totalScore);
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_DistressSummary", scoreSummary);
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_SymptomScore", symptomCount);
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_SymptomSummary", symptomSummary);
});