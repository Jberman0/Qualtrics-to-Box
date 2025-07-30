Qualtrics.SurveyEngine.addOnReady(function() {
    // CSS
    const style = document.createElement('style');
    style.innerHTML = `
        .validation-error { border: 2px solid #ff6b6b !important; background-color: #ffe6e6 !important; }
        .required-indicator { color: #ff6b6b; font-weight: bold; margin-left: 5px; }
        .error-message { color: #ff6b6b; font-size: 12px; margin-top: 5px; display: block; }
        .sbs2-disabled { opacity: 0.5; }
        .sbs2-disabled input { cursor: not-allowed; }
    `;
    document.head.appendChild(style);

    const qc = document.querySelector('.QuestionOuter');

    // Next button validation
    const nextButton = document.querySelector('#NextButton');
    if (nextButton) {
        nextButton.addEventListener('click', function (e) {
            if (!performValidation()) {
                e.preventDefault();
                e.stopPropagation();
                return false;
            }
        }, true);
    }

    // Helpers
    function clearErrors(scope) {
        scope.querySelectorAll('.error-message').forEach(el => el.remove());
        scope.querySelectorAll('input').forEach(i => i.classList.remove('validation-error'));
    }
    function clearSBS2Selection(row) {
        row.querySelectorAll('td.SBS2 input[type="radio"]:checked').forEach(r => {
            r.checked = false;
            r.dispatchEvent(new Event('change', { bubbles: true }));
        });
        row.querySelectorAll('td.SBS2 .AnswerCell.Selected, td.SBS2 .q-checked').forEach(el => el.classList.remove('Selected', 'q-checked'));
        clearErrors(row.querySelector('td.SBS2') || row);
    }
    function setSBS2Disabled(row, disabled) {
        const sbs2Inputs = row.querySelectorAll('td.SBS2 input[type="radio"]');
        const sbs2Cells  = row.querySelectorAll('td.SBS2');
        sbs2Inputs.forEach(i => { i.disabled = disabled; });
        sbs2Cells.forEach(td => td.classList.toggle('sbs2-disabled', disabled));
    }

    // Initialize SBS2 state based on SBS1
    if (qc) {
        qc.querySelectorAll('tr.Choice').forEach((row) => {
            const sbs1 = row.querySelector('td.SBS1 input[type="radio"]:checked');
            const disable = !!(sbs1 && sbs1.value !== "1");
            if (disable) {
                clearSBS2Selection(row);
                setSBS2Disabled(row, true);
            } else {
                setSBS2Disabled(row, false);
            }
        });

        // Live behavior: clear errors, toggle/clear SBS2
        qc.addEventListener('change', function (e) {
            const input = e.target;
            if (!(input && input.matches('input[type="radio"]'))) return;
            const row = input.closest('tr.Choice');
            if (!row) return;

            if (input.closest('td.SBS1')) {
                // SBS1 now satisfied => clear SBS1 errors
                clearErrors(row.querySelector('td.SBS1') || row);

                const isTrue = input.value === "1";
                if (!isTrue) {
                    clearSBS2Selection(row);
                    setSBS2Disabled(row, true);
                } else {
                    setSBS2Disabled(row, false);
                    // If SBS2 already selected, clear its errors
                    if (row.querySelector('td.SBS2 input[type="radio"]:checked')) {
                        clearErrors(row.querySelector('td.SBS2') || row);
                    }
                }
            }

            if (input.closest('td.SBS2')) {
                // SBS2 now satisfied => clear SBS2 errors
                clearErrors(row.querySelector('td.SBS2') || row);
            }
        });
    }
});

Qualtrics.SurveyEngine.addOnUnload(function() {});

function performValidation() {
    const qc = document.querySelector('.QuestionOuter');
    const rows = qc.querySelectorAll('tr.Choice');
    let isValid = true;

    qc.querySelectorAll('.error-message').forEach(msg => msg.remove());
    qc.querySelectorAll('input').forEach(input => input.classList.remove('validation-error'));

    rows.forEach((row) => {
        const sbs1Checked = row.querySelector('.SBS1 input[type="radio"]:checked');
        const sbs2Checked = row.querySelector('.SBS2 input[type="radio"]:checked');

        const sbs1Cell = row.querySelector('.SBS1');
        const sbs2Cell = row.querySelector('.SBS2');

        if (!sbs1Checked) {
            isValid = false;
            const err1 = document.createElement('span');
            err1.className = 'error-message';
            err1.textContent = 'Please select an option';
            sbs1Cell.appendChild(err1);
            row.querySelectorAll('td.SBS1 input').forEach(i => i.classList.add('validation-error'));
        } else if (sbs1Checked.value === "1" && !sbs2Checked) {
            isValid = false;
            const err2 = document.createElement('span');
            err2.className = 'error-message';
            err2.textContent = 'Required when "True" is selected';
            sbs2Cell.appendChild(err2);
            row.querySelectorAll('td.SBS2 input').forEach(i => i.classList.add('validation-error'));
        }
    });

    return isValid;
}

Qualtrics.SurveyEngine.addOnPageSubmit(function () {
  // 1) Validate first; block submit if invalid
  if (typeof performValidation === 'function' && !performValidation()) {
    return false;
  }
  // --------------SCORING----------------
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

  return true; // allow submit
});