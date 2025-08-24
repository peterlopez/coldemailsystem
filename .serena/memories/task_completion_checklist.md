# Task Completion Checklist

When completing any coding task in the Cold Email System project, follow these steps:

## 1. Code Quality Checks
```bash
# Format code with Black
black .

# Run linter to check for issues
flake8 .
```

## 2. Testing
```bash
# Run unit tests (when available)
pytest

# Run integration tests for configuration
python scripts/test_config.py

# Test specific integrations if modified
python scripts/test_bigquery_connection.py  # If BigQuery code changed
python scripts/test_instantly_api.py        # If Instantly code changed
```

## 3. Documentation
- Update docstrings for new/modified functions
- Update README.md if functionality changes
- Update CLAUDE.md if implementation details change

## 4. Pre-Commit Checklist
- [ ] Code is formatted with Black
- [ ] Flake8 passes without errors
- [ ] All tests pass
- [ ] No hardcoded credentials or secrets
- [ ] New dependencies added to requirements.txt
- [ ] Configuration changes documented in .env.example

## 5. Git Workflow
```bash
# Check what changed
git status
git diff

# Stage and commit
git add .
git commit -m "descriptive message"

# Push to remote (only when requested)
git push origin main
```

## 6. Special Considerations
- If adding new Cloud Functions, ensure they follow the functions-framework pattern
- If modifying BigQuery schemas, update the documentation in CLAUDE.md
- If changing API integrations, test with actual API calls
- Always validate configuration changes don't break existing functionality

## Important Notes
- NEVER commit the `config/secrets/` directory
- ALWAYS test configuration loading after environment changes
- Run ALL test scripts before marking task complete
- Ensure error handling is comprehensive for external API calls