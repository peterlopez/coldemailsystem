# Code Style and Conventions

## Python Code Style
- **Python Version**: 3.x (compatible with Google Cloud Functions)
- **Code Formatter**: Black (v23.12.1) - enforces consistent formatting
- **Linter**: Flake8 (v6.1.0) - ensures code quality
- **Import Style**: Standard library, third-party, then local imports
- **Docstrings**: Present on functions/classes, using triple quotes
- **Type Hints**: Used in function signatures (via typing module)
- **Configuration**: Centralized through Config class using property decorators

## Naming Conventions
- **Classes**: PascalCase (e.g., `Config`)
- **Functions**: snake_case (e.g., `test_instantly_connection`, `_load_env`)
- **Constants**: UPPER_SNAKE_CASE (environment variables)
- **Private Methods**: Leading underscore (e.g., `_load_env`)
- **File Names**: snake_case for Python files

## Project Structure Patterns
- Configuration management through a dedicated `Config` class
- Secrets stored in `config/secrets/` directory (git-ignored)
- Test scripts in `scripts/` directory for validation
- Cloud Functions code will go in `functions/` directory
- Environment variables loaded from `.env` file

## Error Handling
- Try-except blocks for external API calls
- Proper error logging and user-friendly messages
- Return codes from test scripts (0 for success, 1 for failure)

## Security Practices
- No hardcoded credentials
- Secrets loaded from files or environment variables
- Credentials directory is git-ignored
- API keys stored in JSON configuration files