/**
 * Ralph Loop Utility Functions
 *
 * Provides utilities for the ralph loop iteration system.
 * Manages ralph state, iteration tracking, completion detection,
 * and failure reporting.
 */

const fs = require('fs');
const path = require('path');
const { readStateFile, writeStateFile, getStateFilePath, sanitizeSpecName } = require('./state-file');

/**
 * Create a ralph state object
 * @param {object} options - Ralph configuration options
 * @param {string} options.mode - Loop mode (default: 'build-validate')
 * @param {number} options.maxIterations - Max iterations, capped at 50 (default: 5)
 * @param {boolean} options.selfHeal - Enable self-healing (default: false)
 * @param {string|null} options.completionPromise - Completion promise text (default: null)
 * @returns {object} Ralph state object
 */
function createRalphState(options = {}) {
  return {
    active: true,
    mode: options.mode || 'build-validate',
    maxIterations: Math.min(options.maxIterations || 5, 50),
    currentIteration: 0,
    selfHeal: options.selfHeal || false,
    completionPromise: options.completionPromise || null,
    startedAt: new Date().toISOString(),
    status: 'active', // active, completed, failed, aborted
    history: [],
    taskRetryCounters: {},
    failureReport: null,
    finalResult: null
  };
}

/**
 * Extract ralph state from a full state object
 * @param {object} state - Full state object
 * @returns {object|null} Ralph state or null if not present
 */
function getRalphState(state) {
  if (!state || !state.ralph) {
    return null;
  }
  return state.ralph;
}

/**
 * Check if the ralph loop is complete
 * @param {object} state - Full state object
 * @returns {boolean} True if ralph is complete (finished, failed, aborted, or max iterations reached)
 */
function isRalphComplete(state) {
  const ralph = getRalphState(state);

  if (!ralph) {
    return false;
  }

  if (ralph.status === 'completed' || ralph.status === 'failed' || ralph.status === 'aborted') {
    return true;
  }

  if (ralph.currentIteration >= ralph.maxIterations) {
    return true;
  }

  return false;
}

/**
 * Check if full validation should be run
 * Returns true only if ALL tasks have status 'completed', 'skipped', or 'stuck'.
 * Returns false if any task is 'pending', 'in-progress', or 'failed'.
 * @param {object} state - Full state object
 * @returns {boolean} True if all tasks are in a terminal state ready for validation
 */
function shouldRunFullValidation(state) {
  if (!state || !state.tasks || state.tasks.length === 0) {
    return false;
  }

  const terminalStatuses = ['completed', 'skipped', 'stuck'];

  return state.tasks.every(task => terminalStatuses.includes(task.status));
}

/**
 * Record an iteration in the ralph loop
 * @param {string} specPath - Path to spec file
 * @param {object} iterationResult - Result of the iteration
 * @param {string} iterationResult.startedAt - ISO timestamp when iteration started
 * @param {string} iterationResult.completedAt - ISO timestamp when iteration completed
 * @param {number} iterationResult.tasksCompleted - Count of tasks completed in this iteration
 * @param {Array} iterationResult.tasksFailed - List of task IDs that failed
 * @param {Array} iterationResult.tasksStuck - List of task IDs that are stuck
 * @param {string} iterationResult.validationResult - Validation outcome (e.g., 'pass', 'fail')
 * @param {string} iterationResult.validationSummary - Human-readable validation summary
 * @returns {object} Updated state object
 */
function updateRalphIteration(specPath, iterationResult) {
  const state = readStateFile(specPath);

  if (!state) {
    throw new Error(`No state file found for ${specPath}`);
  }

  if (!state.ralph) {
    throw new Error(`No ralph state found in state file for ${specPath}`);
  }

  // Increment iteration counter
  state.ralph.currentIteration += 1;

  // Push iteration result to history
  state.ralph.history.push({
    startedAt: iterationResult.startedAt,
    completedAt: iterationResult.completedAt,
    tasksCompleted: iterationResult.tasksCompleted,
    tasksFailed: iterationResult.tasksFailed || [],
    tasksStuck: iterationResult.tasksStuck || [],
    validationResult: iterationResult.validationResult,
    validationSummary: iterationResult.validationSummary
  });

  // Check if max iterations reached
  if (state.ralph.currentIteration >= state.ralph.maxIterations) {
    state.ralph.status = 'failed';
    state.ralph.active = false;
  }

  // Update lastUpdated timestamp
  state.build.lastUpdated = new Date().toISOString();

  // Write updated state
  writeStateFile(specPath, state);

  return state;
}

/**
 * Generate a markdown failure report
 * @param {object} state - Full state object
 * @returns {string} Markdown failure report
 */
function generateFailureReport(state) {
  const ralph = getRalphState(state);

  if (!ralph) {
    return '# Ralph Report\n\nNo ralph state found.\n';
  }

  const specPath = state.build ? state.build.specPath : 'unknown';
  const startedAt = ralph.startedAt || 'unknown';
  const now = new Date().toISOString();
  const duration = ralph.startedAt
    ? formatDuration(new Date(ralph.startedAt), new Date(now))
    : 'unknown';

  let report = '';

  // Header
  report += '# Ralph Loop Report\n\n';
  report += `| Field | Value |\n`;
  report += `|-------|-------|\n`;
  report += `| Spec Path | ${specPath} |\n`;
  report += `| Mode | ${ralph.mode} |\n`;
  report += `| Iterations | ${ralph.currentIteration} / ${ralph.maxIterations} |\n`;
  report += `| Duration | ${duration} |\n`;
  report += `| Status | ${ralph.status} |\n`;
  report += `| Started At | ${startedAt} |\n`;
  report += '\n';

  // Task summary table
  report += '## Task Summary\n\n';

  if (state.tasks && state.tasks.length > 0) {
    report += '| Task | Status | Retries | Last Error |\n';
    report += '|------|--------|---------|------------|\n';

    for (const task of state.tasks) {
      const taskName = task.subject || task.id;
      const retries = ralph.taskRetryCounters[task.id] || 0;
      const lastError = getLastErrorForTask(ralph, task.id);
      report += `| ${taskName} | ${task.status} | ${retries} | ${lastError} |\n`;
    }

    report += '\n';
  } else {
    report += 'No tasks found.\n\n';
  }

  // Iteration history table
  report += '## Iteration History\n\n';

  if (ralph.history && ralph.history.length > 0) {
    report += '| # | Started | Completed | Tasks Done | Failed | Stuck | Validation |\n';
    report += '|---|---------|-----------|------------|--------|-------|------------|\n';

    ralph.history.forEach((entry, index) => {
      const started = entry.startedAt ? entry.startedAt.substring(11, 19) : '-';
      const completed = entry.completedAt ? entry.completedAt.substring(11, 19) : '-';
      const failed = entry.tasksFailed ? entry.tasksFailed.join(', ') : '-';
      const stuck = entry.tasksStuck ? entry.tasksStuck.join(', ') : '-';
      const validation = entry.validationResult || '-';
      report += `| ${index + 1} | ${started} | ${completed} | ${entry.tasksCompleted || 0} | ${failed} | ${stuck} | ${validation} |\n`;
    });

    report += '\n';
  } else {
    report += 'No iteration history recorded.\n\n';
  }

  // Suggested next steps
  report += '## Suggested Next Steps\n\n';
  report += generateSuggestedSteps(state, ralph);

  return report;
}

/**
 * Write the failure report to disk
 * @param {string} specPath - Path to spec file
 * @param {string} report - Markdown report content
 */
function writeFailureReport(specPath, report) {
  const stateFilePath = getStateFilePath(specPath);
  const specDir = path.dirname(stateFilePath);
  const reportPath = path.join(specDir, 'ralph-report.md');

  try {
    // Create directory if it doesn't exist
    if (!fs.existsSync(specDir)) {
      fs.mkdirSync(specDir, { recursive: true });
    }

    fs.writeFileSync(reportPath, report, 'utf8');
  } catch (error) {
    throw new Error(`Failed to write failure report to ${reportPath}: ${error.message}`);
  }
}

// --- Internal Helpers ---

/**
 * Format duration between two dates as a human-readable string
 * @param {Date} start - Start date
 * @param {Date} end - End date
 * @returns {string} Formatted duration (e.g., "2m 30s", "1h 5m")
 */
function formatDuration(start, end) {
  const diffMs = end.getTime() - start.getTime();

  if (diffMs < 0) return '0s';

  const seconds = Math.floor(diffMs / 1000) % 60;
  const minutes = Math.floor(diffMs / (1000 * 60)) % 60;
  const hours = Math.floor(diffMs / (1000 * 60 * 60));

  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

/**
 * Get the last error message for a task from ralph history
 * @param {object} ralph - Ralph state
 * @param {string} taskId - Task ID
 * @returns {string} Last error or '-'
 */
function getLastErrorForTask(ralph, taskId) {
  if (!ralph.history || ralph.history.length === 0) return '-';

  // Walk history in reverse to find the last iteration where this task failed
  for (let i = ralph.history.length - 1; i >= 0; i--) {
    const entry = ralph.history[i];
    if (entry.tasksFailed && entry.tasksFailed.includes(taskId)) {
      return entry.validationSummary || 'Failed (no details)';
    }
  }

  return '-';
}

/**
 * Generate suggested next steps based on failure patterns
 * @param {object} state - Full state object
 * @param {object} ralph - Ralph state
 * @returns {string} Markdown text with suggestions
 */
function generateSuggestedSteps(state, ralph) {
  const suggestions = [];

  // Check for persistent failures (same tasks failing across iterations)
  const failureCounts = {};
  if (ralph.history) {
    for (const entry of ralph.history) {
      if (entry.tasksFailed) {
        for (const taskId of entry.tasksFailed) {
          failureCounts[taskId] = (failureCounts[taskId] || 0) + 1;
        }
      }
    }
  }

  const persistentFailures = Object.entries(failureCounts)
    .filter(([, count]) => count >= 2);

  if (persistentFailures.length > 0) {
    const taskIds = persistentFailures.map(([id]) => id).join(', ');
    suggestions.push(
      `- **Persistent failures detected** in tasks: ${taskIds}. These tasks have failed across multiple iterations. Consider a different approach, breaking them into smaller tasks, or reviewing their requirements.`
    );
  }

  // Check for stuck tasks
  if (state.tasks) {
    const stuckTasks = state.tasks.filter(t => t.status === 'stuck');
    if (stuckTasks.length > 0) {
      const stuckIds = stuckTasks.map(t => t.id || t.subject).join(', ');
      suggestions.push(
        `- **Stuck tasks** detected: ${stuckIds}. These tasks could not make progress. Check for circular dependencies or missing prerequisites.`
      );
    }
  }

  // Check if max iterations were exhausted
  if (ralph.currentIteration >= ralph.maxIterations) {
    suggestions.push(
      `- **Max iterations reached** (${ralph.maxIterations}). The loop could not complete within the iteration limit. Consider increasing maxIterations or simplifying the spec.`
    );
  }

  // Check for validation failures
  const validationFailures = (ralph.history || []).filter(
    e => e.validationResult === 'fail'
  );
  if (validationFailures.length > 0) {
    suggestions.push(
      `- **Validation failed** in ${validationFailures.length} iteration(s). Review validation criteria and ensure they are achievable with the current implementation.`
    );
  }

  // Default suggestion if none of the above
  if (suggestions.length === 0) {
    suggestions.push(
      '- Review the spec and task definitions for clarity.',
      '- Consider running with `selfHeal: true` to enable automatic recovery.',
      '- Check agent logs for detailed error information.'
    );
  }

  return suggestions.join('\n') + '\n';
}

module.exports = {
  createRalphState,
  getRalphState,
  isRalphComplete,
  shouldRunFullValidation,
  updateRalphIteration,
  generateFailureReport,
  writeFailureReport
};
