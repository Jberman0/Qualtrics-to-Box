(function () {
  const thresholds = {
    distress: 9,
    symptom: 6
  };

  const symptomMap = { "1": 1, "2": 0 };
  const distressMap = { "1": 0, "2": 1, "3": 2, "4": 3 };

  function clearErrors(scope) {
    scope.querySelectorAll('.error-message').forEach(el => el.remove());
    scope.querySelectorAll('input').forEach(i => i.classList.remove('validation-error'));
  }

  function clearSBS2Selection(row) {
    row.querySelectorAll('td.SBS2 input[type="radio"]:checked').forEach(r => {
      r.checked = false;
      r.dispatchEvent(new Event('change', { bubbles: true }));
    });
    row.querySelectorAll('td.SBS2 .AnswerCell.Selected, td.SBS2 .q-checked')
       .forEach(el => el.classList.remove('Selected', 'q-checked'));
    const sbs2Cell = row.querySelector('td.SBS2') || row;
    clearErrors(sbs2Cell);
  }

  function setSBS2Disabled(row, disabled) {
    const sbs2Inputs = row.querySelectorAll('td.SBS2 input[type="radio"]');
    const sbs2Cells = row.querySelectorAll('td.SBS2');
    sbs2Inputs.forEach(i => { i.disabled = disabled; });
    sbs2Cells.forEach(td => td.classList.toggle('sbs2-disabled', disabled));
  }

  function initializeSBS2State(qc) {
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
  }

  function performValidation(qc) {
    let isValid = true;

    qc.querySelectorAll('.error-message').forEach(msg => msg.remove());
    qc.querySelectorAll('input').forEach(input => input.classList.remove('validation-error'));

    qc.querySelectorAll('tr.Choice').forEach((row) => {
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

  function computeAndSetScores(qc) {
    let distressScore = 0;
    let symptomCount = 0;

    qc.querySelectorAll('tr.Choice').forEach((row) => {
      const sbs1 = row.querySelector('.SBS1 input[type="radio"]:checked');
      const sbs2 = row.querySelector('.SBS2 input[type="radio"]:checked');

      const symptomValue = (sbs1 && symptomMap[sbs1.value] !== undefined)
        ? symptomMap[sbs1.value] : 0;
      symptomCount += symptomValue;

      if (symptomValue === 1) {
        const distressValue = (sbs2 && distressMap[sbs2.value] !== undefined)
          ? distressMap[sbs2.value] : 0;
        distressScore += distressValue;
      }
    });

    const distressSummary = distressScore >= thresholds.distress ? "Significant" : "Not Significant";
    const symptomSummary = symptomCount >= thresholds.symptom ? "Significant" : "Not Significant";

    Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_DistressScore", distressScore);
    Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_DistressSummary", distressSummary);
    Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_SymptomScore", symptomCount);
    Qualtrics.SurveyEngine.setEmbeddedData("PQ-16_SymptomSummary", symptomSummary);
  }

  Qualtrics.SurveyEngine.addOnReady(function () {
    const qc = this.getQuestionContainer();

    if (!document.querySelector('#pq16-validation-style')) {
      const style = document.createElement('style');
      style.id = 'pq16-validation-style';
      style.innerHTML = `
        .validation-error { border: 2px solid #ff6b6b !important; background-color: #ffe6e6 !important; }
        .required-indicator { color: #ff6b6b; font-weight: bold; margin-left: 5px; }
        .error-message { color: #ff6b6b; font-size: 12px; margin-top: 5px; display: block; }
        .sbs2-disabled { opacity: 0.5; }
        .sbs2-disabled input { cursor: not-allowed; }
      `;
      document.head.appendChild(style);
    }

    initializeSBS2State(qc);

    qc.addEventListener('change', function (e) {
      const input = e.target;
      if (!(input && input.matches('input[type="radio"]'))) return;

      const row = input.closest('tr.Choice');
      if (!row) return;

      if (input.closest('td.SBS1')) {
        clearErrors(row.querySelector('td.SBS1') || row);
        const isTrue = input.value === "1";
        if (!isTrue) {
          clearSBS2Selection(row);
          setSBS2Disabled(row, true);
        } else {
          setSBS2Disabled(row, false);
          if (row.querySelector('td.SBS2 input[type="radio"]:checked')) {
            clearErrors(row.querySelector('td.SBS2') || row);
          }
        }
      }

      if (input.closest('td.SBS2')) {
        clearErrors(row.querySelector('td.SBS2') || row);
      }
    });

    const nextButton = document.querySelector('#NextButton');
    if (nextButton) {
      nextButton.addEventListener('click', function (e) {
        if (!performValidation(qc)) {
          e.preventDefault();
          e.stopPropagation();
          return false;
        }
      }, true);
    }
  });

  Qualtrics.SurveyEngine.addOnPageSubmit(function () {
    const qc = this.getQuestionContainer();

    if (!performValidation(qc)) {
      return false;
    }

    computeAndSetScores(qc);
    return true;
  });

})();