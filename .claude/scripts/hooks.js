/**
 * Validation Hooks Helper
 *
 * Parses and executes YAML-based validation hooks from spec files.
 * Hooks run before marking tasks as complete, ensuring quality.
 *
 * Hook format in spec:
 * ```yaml
 * stop:
 *   - type: agent_output
 *     validate: success | no_errors | contains_text
 *     pattern: "text to find"  # for contains_text
 *     on_failure: retry | fail | continue
 *   - type: command
 *     command: <shell command>
 *     expect: exit_code_0 | stdout_contains | stderr_empty
 *     pattern: "expected output"  # for stdout_contains
 *     timeout: 30000
 *   - type: artifact
 *     path: <relative file path>
 *     exists: true | false
 *     content_includes: "text"
 *   - type: acceptance_criteria
 *     criteria:
 *       - "criterion 1"
 *       - "criterion 2"
 *     require: all | any
 *     source: spec  # read from spec's checklist
 * ```
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

/**
 * Extract hooks YAML from a task section in a spec
 * @param {string} specContent - Full spec file content
 * @param {string} taskSubject - Task subject/identifier
 * @returns {object|null} Parsed hooks object or null if no hooks
 */
function extractTaskHooks(specContent, taskSubject) {
  // Find the task section by matching the task subject line
  // Pattern: "### Task N: <subject>" or "- [ ] Task N: <subject>"
  const taskPattern = new RegExp(
    `(?:### |\\- \\[ \\] )Task\\s+\\d+:\\s*${escapeRegex(taskSubject)}[\\s\\S]*?(?=### |\\- \\[ \\] Task\\s+\\d+:|$)`,
    'i'
  );

  const taskMatch = specContent.match(taskPattern);
  if (!taskMatch) {
    return null;
  }

  const taskSection = taskMatch[0];

  // Find YAML block after "**Validation:**" or "**Hooks:**"
  const yamlPattern = /(?:\\*\\*Validation:\\*\\*|\\*\\*Hooks:\\*\\*)[\\s\\S]*?```yaml\\n([\\s\\S]*?)```/i;
  const yamlMatch = taskSection.match(yamlPattern);

  if (!yamlMatch) {
    return null;
  }

  const yamlContent = yamlMatch[1];
  return parseHooksYAML(yamlContent);
}

/**
 * Parse YAML hooks content into structured object
 * @param {string} yamlContent - Raw YAML content
 * @returns {object} Parsed hooks object
 */
function parseHooksYAML(yamlContent) {
  const hooks = { stop: [] };

  const lines = yamlContent.split('\n');
  let currentList = null;
  let currentHook = null;
  let indentLevel = 0;

  for (const line of lines) {
    const trimmed = line.trim();

    // Skip empty lines and comments
    if (!trimmed || trimmed.startsWith('#')) continue;

    // Detect "stop:" keyword
    if (trimmed === 'stop:') {
      currentList = 'stop';
      continue;
    }

    // Detect list item "- type:"
    if (trimmed.startsWith('- type:')) {
      if (currentHook && currentList) {
        hooks[currentList].push(currentHook);
      }
      currentHook = { type: trimmed.split(':')[1].trim() };
      continue;
    }

    // Parse hook properties
    if (currentHook && trimmed.includes(':')) {
      const [key, ...valueParts] = trimmed.split(':');
      const value = valueParts.join(':').trim();

      // Handle different value types
      if (value.startsWith('"') || value.startsWith("'")) {
        // String value
        currentHook[key.trim()] = value.slice(1, -1);
      } else if (value === 'true' || value === 'false') {
        // Boolean value
        currentHook[key.trim()] = value === 'true';
      } else if (value.startsWith('-')) {
        // Start of list (for criteria)
        currentHook[key.trim()] = [];
      } else if (!isNaN(parseInt(value))) {
        // Number value
        currentHook[key.trim()] = parseInt(value);
      } else {
        currentHook[key.trim()] = value;
      }
    }

    // Handle list items (for criteria array)
    if (currentHook && trimmed.startsWith('- ') && currentHook.criteria) {
      const criterion = trimmed.slice(2).trim().replace(/['"]/g, '');
      currentHook.criteria.push(criterion);
    }
  }

  // Push the last hook
  if (currentHook && currentList) {
    hooks[currentList].push(currentHook);
  }

  return hooks;
}

/**
 * Run all validation hooks for a task
 * @param {Array} hooks - Array of hook objects
 * @param {object} context - Execution context
 * @param {string} context.agentOutput - Agent's output text
 * @param {string} context.taskId - Task ID
 * @param {string} context.specPath - Path to spec file
 * @param {string} context.workingDir - Working directory for commands
 * @returns {Promise<object>} Validation results
 */
async function runHooks(hooks, context = {}) {
  const results = {
    total: hooks.length,
    passed: 0,
    failed: 0,
    errors: [],
    details: []
  };

  for (const hook of hooks) {
    try {
      const result = await runSingleHook(hook, context);
      results.details.push({ hook, result });

      if (result.passed) {
        results.passed++;
      } else {
        results.failed++;
        results.errors.push(result.error || `Hook ${hook.type} failed`);
      }

      // Handle on_failure behavior
      if (!result.passed && hook.on_failure === 'fail') {
        break; // Stop running more hooks
      }
    } catch (error) {
      results.failed++;
      results.errors.push(error.message);
      results.details.push({ hook, error: error.message });
    }
  }

  return results;
}

/**
 * Run a single validation hook
 * @param {object} hook - Hook configuration
 * @param {object} context - Execution context
 * @returns {Promise<object>} Hook result
 */
async function runSingleHook(hook, context) {
  switch (hook.type) {
    case 'agent_output':
      return validateAgentOutput(hook, context.agentOutput || '');

    case 'command':
      return validateCommand(hook, context.workingDir || process.cwd());

    case 'artifact':
      return validateArtifact(hook, context.workingDir || process.cwd());

    case 'acceptance_criteria':
      return validateAcceptanceCriteria(hook, context.specPath);

    default:
      return { passed: false, error: `Unknown hook type: ${hook.type}` };
  }
}

/**
 * Validate agent output
 */
function validateAgentOutput(hook, output) {
  const { validate, pattern } = hook;

  switch (validate) {
    case 'success':
      // Check if output doesn't contain error indicators
      const errorIndicators = ['error:', 'failed', 'exception', 'cannot', 'unable to'];
      const hasErrors = errorIndicators.some(indicator =>
        output.toLowerCase().includes(indicator)
      );
      return { passed: !hasErrors, error: hasErrors ? 'Output contains error indicators' : null };

    case 'no_errors':
      // More strict - no "error" or "Error" anywhere
      const hasError = output.toLowerCase().includes('error');
      return { passed: !hasError, error: hasError ? 'Output contains "error"' : null };

    case 'contains_text':
      if (!pattern) {
        return { passed: false, error: 'pattern required for contains_text validation' };
      }
      const contains = output.includes(pattern);
      return { passed: contains, error: contains ? null : `Output does not contain "${pattern}"` };

    default:
      return { passed: false, error: `Unknown validation type: ${validate}` };
  }
}

/**
 * Validate command execution
 */
function validateCommand(hook, workingDir) {
  const { command, expect = 'exit_code_0', timeout = 30000 } = hook;

  try {
    const result = execSync(command, {
      cwd: workingDir,
      timeout,
      encoding: 'utf-8',
      stdio: 'pipe'
    });

    switch (expect) {
      case 'exit_code_0':
        return { passed: true };

      case 'stdout_contains':
        if (!hook.pattern) {
          return { passed: false, error: 'pattern required for stdout_contains' };
        }
        const contains = result.includes(hook.pattern);
        return { passed: contains, error: contains ? null : `stdout does not contain "${hook.pattern}"` };

      case 'stderr_empty':
        return { passed: true, error: null };

      default:
        return { passed: false, error: `Unknown expect type: ${expect}` };
    }
  } catch (error) {
    return { passed: false, error: error.message };
  }
}

/**
 * Validate artifact existence and content
 */
function validateArtifact(hook, workingDir) {
  const { path: artifactPath, exists, content_includes } = hook;
  const fullPath = path.resolve(workingDir, artifactPath);

  // Check existence
  const fileExists = fs.existsSync(fullPath);

  if (exists === true && !fileExists) {
    return { passed: false, error: `Artifact does not exist: ${artifactPath}` };
  }

  if (exists === false && fileExists) {
    return { passed: false, error: `Artifact exists but should not: ${artifactPath}` };
  }

  if (exists === true && fileExists && !content_includes) {
    return { passed: true };
  }

  // Check content if specified
  if (content_includes && fileExists) {
    const content = fs.readFileSync(fullPath, 'utf-8');
    const contains = content.includes(content_includes);
    return { passed: contains, error: contains ? null : `File does not contain "${content_includes}"` };
  }

  return { passed: true };
}

/**
 * Validate acceptance criteria from spec
 */
function validateAcceptanceCriteria(hook, specPath) {
  const { criteria, require = 'all', source = 'spec' } = hook;

  if (source !== 'spec') {
    return { passed: false, error: `Only "spec" source is currently supported` };
  }

  // Read spec file
  const specContent = fs.readFileSync(specPath, 'utf-8');

  // Find acceptance criteria section
  const acSection = specContent.match(/## Acceptance Criteria\\n([\\s\\S]*?)(?=##|$)/i);
  if (!acSection) {
    return { passed: false, error: 'No Acceptance Criteria section found in spec' };
  }

  const sectionContent = acSection[1];
  const checkedCriteria = [];

  // Extract all checked items (- [x] ...)
  const checkboxPattern = /-\\s*\\[x\\]\\s*(.+)/g;
  let match;
  while ((match = checkboxPattern.exec(sectionContent)) !== null) {
    checkedCriteria.push(match[1].trim().toLowerCase());
  }

  // Validate criteria
  if (require === 'all') {
    const allMet = criteria.every(c =>
      checkedCriteria.some(checked => checked.includes(c.toLowerCase()))
    );
    return {
      passed: allMet,
      error: allMet ? null : `Not all criteria met. Checked: ${checkedCriteria.length}, Required: ${criteria.length}`
    };
  } else if (require === 'any') {
    const anyMet = criteria.some(c =>
      checkedCriteria.some(checked => checked.includes(c.toLowerCase()))
    );
    return {
      passed: anyMet,
      error: anyMet ? null : `None of the criteria met. Checked: ${checkedCriteria.join(', ')}`
    };
  }

  return { passed: false, error: `Unknown require type: ${require}` };
}

/**
 * Helper: Escape special regex characters
 */
function escapeRegex(string) {
  return string.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&');
}

module.exports = {
  extractTaskHooks,
  parseHooksYAML,
  runHooks,
  runSingleHook,
  validateAgentOutput,
  validateCommand,
  validateArtifact,
  validateAcceptanceCriteria
};
