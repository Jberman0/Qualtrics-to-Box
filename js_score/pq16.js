Qualtrics.SurveyEngine.addOnPageSubmit(function() {
  // MUST ADD THESE AS EMBEDDED DATA BEFORE THIS BLOCK:
  // "PQ-16_DistressScore", "PQ-16_DistressSummary", "PQ-16_SymptomScore", "PQ-16_SymptomSummary"

  // ── Configuration: Scoring settings ─────────────
  const thresholds = {
    distress: 9,  // Threshold for Distress Score
    symptom: 6    // Threshold for Symptom Count
  };

  // ── Scoring calculation ─────────────────────────
  let distressScore = 0;
  let symptomCount = 0;

  // Predefine maps 
  const symptomMap  = { "1": 1, "2": 0 };
  const distressMap = { "1": 0, "2": 1, "3": 2, "4": 3 };

  const questionContainer = this.getQuestionContainer();
  const allRows = questionContainer.querySelectorAll('tr');

  allRows.forEach((row) => {
    if (!row.classList.contains('Choice')) return;

    const selectedSymptomInput  = row.querySelector('.SBS1 input[type="radio"]:checked');
    const selectedDistressInput = row.querySelector('.SBS2 input[type="radio"]:checked');

    const symptomValue = (selectedSymptomInput && symptomMap[selectedSymptomInput.value] !== undefined)
    ? symptomMap[selectedSymptomInput.value]
    : 0;
    symptomCount  += symptomValue;
    if (symptomValue === 0) return; // Skip distress calculation if symptom is false

    const distressValue =(selectedDistressInput && distressMap[selectedDistressInput.value] !== undefined)
    ? distressMap[selectedDistressInput.value]
    : 0;
    distressScore += distressValue;
  });

  // Determine summaries
  const distressSummary = distressScore >= thresholds.distress ? "Significant" : "Not Significant";
  const symptomSummary  = symptomCount  >= thresholds.symptom  ? "Significant" : "Not Significant";

  // ── Set embedded data ──────────────────────────
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_DistressScore", distressScore);
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_DistressSummary", distressSummary);
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_SymptomScore",  symptomCount);
  Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_SymptomSummary", symptomSummary);

  // Optional: Console logging for debugging (remove in production)
  console.log("PQ-16 Scoring Results:", {
    distressScore: distressScore,
    symptomCount: symptomCount,
    distressSummary: distressSummary,
    symptomSummary: symptomSummary
  });
});