# aiida-jutools Test Suite

Comprehensive test suite for the aiida-jutools package using pytest.

## Quick Start

### Installing Test Dependencies

First, install pytest and optional testing tools:

```bash
# Minimal installation
pip install pytest

# Recommended installation with coverage
pip install pytest pytest-cov

# Optional: for better output formatting
pip install pytest-sugar
```

### Running Tests

**Basic test run (all tests):**
```bash
pytest
```

**Run with verbose output:**
```bash
pytest -v
```

**Run only unit tests (fast, no database required):**
```bash
pytest -m unit
```

**Run with coverage report:**
```bash
pytest --cov=aiida_jutools --cov-report=html
# Opens htmlcov/index.html in browser to view coverage
```

**Run with deprecation warnings as errors:**
```bash
pytest --strict-deprecation
```

**Run integration tests with a specific computer:**
```bash
pytest -m integration --with-computer=iffslurm
```

**Run and stop at first failure:**
```bash
pytest -x
```

**Run specific test file:**
```bash
pytest tests/test_computer_util.py
```

**Run specific test function:**
```bash
pytest tests/test_computer_util.py::test_is_slurm_computer_with_core_slurm
```

## Test Organization

### Test Markers

Tests are organized using pytest markers:

- `@pytest.mark.unit` - Fast unit tests, no external dependencies
- `@pytest.mark.integration` - Integration tests requiring AiiDA database
- `@pytest.mark.requires_computer` - Tests requiring configured AiiDA computer
- `@pytest.mark.slow` - Tests that take longer to run (e.g., cluster queries)

**Running tests by marker:**
```bash
# Run only unit tests
pytest -m unit

# Run integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"

# Run unit tests but not slow ones
pytest -m "unit and not slow"
```

### Test File Structure

```
tests/
├── __init__.py              # Test package marker
├── conftest.py              # Pytest fixtures and configuration
├── test_computer_util.py    # Tests for computer utilities
├── test_code_util.py        # Tests for code utilities (add this)
└── README.md                # This file
```

## Configuration Files

### pytest.ini

Main configuration file at repository root. Controls:
- Test discovery patterns
- Warning filters
- Default options
- Test markers

**Key settings:**
```ini
[pytest]
testpaths = tests          # Where to find tests
filterwarnings = default   # Show all warnings by default
```

### conftest.py

Contains:
- **Fixtures**: Reusable test setup (e.g., `mock_computer`, `aiida_profile`)
- **Command-line options**: Custom flags like `--strict-deprecation`
- **Hooks**: Test configuration hooks

## Deprecation Warning Control

### Method 1: Command-line flag (Recommended for Development)

```bash
# Fail on deprecation warnings
pytest --strict-deprecation

# Normal mode (show warnings but don't fail)
pytest
```

### Method 2: Environment variable

```bash
# Set for session
export PYTEST_ADDOPTS="--strict-deprecation"
pytest

# Or inline
PYTEST_ADDOPTS="--strict-deprecation" pytest
```

### Method 3: Edit pytest.ini (Permanent)

Uncomment in `pytest.ini`:
```ini
filterwarnings =
    error::DeprecationWarning  # Fail on deprecation warnings
```

## Writing Tests

### Example Unit Test (No Database Required)

```python
import pytest

@pytest.mark.unit
def test_is_slurm_computer(mock_computer):
    """Test is_slurm_computer with mock computer."""
    from aiida_jutools.computer.util import is_slurm_computer

    mock_computer.scheduler_type = "core.slurm"
    assert is_slurm_computer(mock_computer) is True
```

### Example Integration Test (Requires Database)

```python
@pytest.mark.integration
@pytest.mark.requires_computer
def test_get_queues(computer_label, aiida_profile):
    """Test get_queues with real AiiDA computer."""
    from aiida_jutools.computer.util import get_computers, get_queues

    computers = get_computers(computer_label)
    queues = get_queues(computers[0], with_node_count=False, silent=True)

    assert len(queues) > 0
```

### Test Structure Best Practices

1. **One assertion per test** (when possible)
2. **Use descriptive test names** (`test_function_with_condition_expects_result`)
3. **Use fixtures** for setup/teardown
4. **Mark tests appropriately** (unit/integration/slow)
5. **Test edge cases** (empty input, None, errors)

## IDE Integration

### PyCharm

1. **Enable pytest:**
   - Go to: Settings → Tools → Python Integrated Tools
   - Set "Default test runner" to "pytest"
   - Apply and restart

2. **Run tests:**
   - Right-click on test file/function → "Run pytest in..."
   - Use green play button in gutter next to test
   - View results in "Run" tool window

3. **Run with options:**
   - Edit run configuration
   - Add to "Additional Arguments": `--strict-deprecation -v`

4. **View coverage:**
   - Right-click test file → "Run with Coverage"

### VSCode

1. **Install Python extension** (Microsoft)

2. **Configure pytest:**
   - Open Command Palette (Cmd/Ctrl+Shift+P)
   - Type "Python: Configure Tests"
   - Select "pytest"
   - Select "tests" directory

3. **Run tests:**
   - Click test icon in activity bar (beaker icon)
   - Click play button next to test/file
   - Use Testing panel to view results

4. **Configure settings.json:**
   ```json
   {
       "python.testing.pytestEnabled": true,
       "python.testing.pytestArgs": [
           "tests",
           "-v"
       ]
   }
   ```

### JupyterLab

While not ideal for running full test suites, you can run tests from notebook:

```python
# Install ipytest
!pip install ipytest

# In notebook cell:
import ipytest
ipytest.autoconfig()

%%ipytest -v

def test_something():
    assert 1 + 1 == 2
```

**Better approach for Jupyter:**
```python
# Run tests from notebook using subprocess
import subprocess
result = subprocess.run(['pytest', 'tests/', '-v'],
                       capture_output=True, text=True)
print(result.stdout)
```

## Development Workflow

### Test-Driven Development (TDD) Cycle

1. **Write a failing test** for new feature
   ```bash
   pytest tests/test_new_feature.py -v
   # Test should fail (red)
   ```

2. **Write minimal code** to make it pass
   ```python
   # Implement feature in aiida_jutools/
   ```

3. **Run test again**
   ```bash
   pytest tests/test_new_feature.py -v
   # Test should pass (green)
   ```

4. **Refactor** code while keeping tests green

5. **Repeat** for next feature

### Pre-commit Workflow

Before committing code:

```bash
# Run all unit tests (fast)
pytest -m unit

# If adding integration features, test those too
pytest -m integration --with-computer=iffslurm

# Check for deprecation warnings
pytest --strict-deprecation

# Run with coverage
pytest --cov=aiida_jutools --cov-report=term-missing
```

### Continuous Development

**Watch mode** (requires pytest-watch):
```bash
pip install pytest-watch
ptw tests/  # Re-runs tests on file changes
```

**Run tests in parallel** (requires pytest-xdist):
```bash
pip install pytest-xdist
pytest -n auto  # Uses all CPU cores
```

## Common Patterns

### Testing Exceptions

```python
def test_function_raises_error():
    """Test that function raises ValueError."""
    from aiida_jutools.computer.util import get_queues

    with pytest.raises(NotImplementedError, match="only works with SLURM"):
        get_queues(non_slurm_computer)
```

### Parametrized Tests

```python
@pytest.mark.parametrize("scheduler_type,expected", [
    ("core.slurm", True),
    ("slurm", True),
    ("core.pbs", False),
    ("core.sge", False),
])
def test_is_slurm_computer_various_types(mock_computer, scheduler_type, expected):
    """Test is_slurm_computer with various scheduler types."""
    from aiida_jutools.computer.util import is_slurm_computer

    mock_computer.scheduler_type = scheduler_type
    assert is_slurm_computer(mock_computer) == expected
```

### Skipping Tests

```python
@pytest.mark.skip(reason="Feature not implemented yet")
def test_future_feature():
    pass

@pytest.mark.skipif(sys.platform == "win32", reason="Unix only")
def test_unix_feature():
    pass
```

## Troubleshooting

### "No module named aiida_jutools"

Install package in development mode:
```bash
pip install -e .
```

### "AiiDA profile not available"

Tests requiring AiiDA will auto-skip. To run them:
```bash
# Ensure AiiDA is configured
verdi status

# Run with specific profile
verdi -p <profile_name> run pytest
```

### "Collection fixtures not working"

Ensure conftest.py is in tests/ directory and tests/__init__.py exists.

### pytest not finding tests

Check pytest.ini configuration and ensure test files start with `test_`.

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [pytest markers](https://docs.pytest.org/en/stable/mark.html)
- [Testing best practices](https://docs.pytest.org/en/stable/goodpractices.html)

## Next Steps

1. **Run the test suite** and ensure it works in your environment
2. **Add more tests** as you develop new features
3. **Set up CI/CD** (GitHub Actions, GitLab CI) to run tests automatically
4. **Monitor coverage** and aim for >80% code coverage
5. **Update tests** when refactoring to prevent regressions