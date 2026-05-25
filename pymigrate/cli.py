import os
import sys
import click
import yaml
import logging
from pymigrate.templates import MigrationTemplate, ValidationError
from pymigrate.engine import MigrationEngine, MigrationError

def setup_logging(verbose: bool) -> None:
    """Configure console logging level and format."""
    level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=log_format, stream=sys.stdout)
    
    # Set third-party drivers to warning to avoid unnecessary noise unless high verbose
    logging.getLogger("oracledb").setLevel(logging.WARNING)
    logging.getLogger("psycopg2").setLevel(logging.WARNING)


def load_db_config(config_path: str) -> dict:
    """Helper to load the database credentials configuration file."""
    if not os.path.exists(config_path):
        click.secho(f"Error: Database config file not found at {config_path}", fg="red", err=True)
        sys.exit(1)
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            if not isinstance(data, dict) or "databases" not in data:
                click.secho("Error: Database config file must contain a 'databases' dictionary.", fg="red", err=True)
                sys.exit(1)
            return data["databases"]
    except Exception as e:
        click.secho(f"Error reading database config file: {e}", fg="red", err=True)
        sys.exit(1)


@click.group()
@click.version_option(version="0.1.0")
def main():
    """py-migrate: Declarative database data migration tool."""
    pass


@main.command()
@click.argument("template_path", type=click.Path(exists=True))
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to database credentials config YAML.")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose debug logging.")
def validate(template_path, config, verbose):
    """Validate template structure and dry-run DB connections."""
    setup_logging(verbose)
    click.secho("==================================================", fg="cyan")
    click.secho(f"Validating template: {template_path}", fg="cyan", bold=True)
    click.secho("==================================================", fg="cyan")

    try:
        template = MigrationTemplate(template_path)
        click.secho("✓ Template parsed and structure is valid.", fg="green")
        click.echo(f"  Migration Name: {template.name}")
        click.echo(f"  Source Database: {template.source_db}")
        click.echo(f"  Target Database: {template.target_db}")
        click.echo(f"  Target Table: {template.target_table}")
        click.echo(f"  Mapped Columns: {len(template.columns)}")
        
        db_configs = load_db_config(config)
        engine = MigrationEngine(template, db_configs)
        
        results = engine.validate_only()
        
        click.secho("\n--- Pre-Migration Checklist (Dry-Run) ---", fg="yellow")
        for check_res in results["pre_checklist"]:
            if check_res.passed:
                click.secho(f"  ✓ {check_res.name} (Passed)", fg="green")
            else:
                click.secho(f"  ✗ {check_res.name} (Failed) - {check_res.message}", fg="red")
        
        if results["success"]:
            click.secho("\n✓ DRY-RUN VALIDATION SUCCESSFUL!", fg="green", bold=True)
            sys.exit(0)
        else:
            click.secho("\n✗ DRY-RUN VALIDATION FAILED (Pre-checks failed).", fg="red", bold=True)
            sys.exit(1)

    except ValidationError as e:
        click.secho(f"\nTemplate Validation Error: {e}", fg="red", bold=True, err=True)
        sys.exit(1)
    except MigrationError as e:
        click.secho(f"\nMigration Validation Error: {e}", fg="red", bold=True, err=True)
        sys.exit(1)
    except Exception as e:
        click.secho(f"\nUnexpected error: {e}", fg="red", bold=True, err=True)
        sys.exit(1)


@main.command()
@click.argument("template_path", type=click.Path(exists=True))
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to database credentials config YAML.")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose debug logging.")
@click.option("--dry-run", is_flag=True, help="Perform dry-run connection and pre-checks without executing migration.")
def run(template_path, config, verbose, dry_run):
    """Execute data migration based on the provided template."""
    setup_logging(verbose)
    
    if dry_run:
        click.echo("Running in dry-run mode...")
        ctx = click.get_current_context()
        ctx.invoke(validate, template_path=template_path, config=config, verbose=verbose)
        return

    click.secho("==================================================", fg="cyan")
    click.secho(f"Starting Migration: {template_path}", fg="cyan", bold=True)
    click.secho("==================================================", fg="cyan")

    try:
        template = MigrationTemplate(template_path)
        db_configs = load_db_config(config)
        
        engine = MigrationEngine(template, db_configs)
        
        # Execute migration
        result = engine.run()
        
        # Print results nicely
        click.secho("\n==================================================", fg="green")
        click.secho("MIGRATION COMPLETED SUCCESSFULLY!", fg="green", bold=True)
        click.secho("==================================================", fg="green")
        click.echo(f"Total Rows Migrated: {result['rows_migrated']}")
        
        click.secho("\n--- Pre-Migration Checklist Results ---", fg="cyan")
        for res in result["pre_checklist"]:
            click.secho(f"  ✓ {res.name} (Passed)", fg="green")

        click.secho("\n--- Post-Migration Checklist Results ---", fg="cyan")
        for res in result["post_checklist"]:
            click.secho(f"  ✓ {res.name} (Passed)", fg="green")
            
        sys.exit(0)

    except ValidationError as e:
        click.secho(f"\nTemplate Validation Error: {e}", fg="red", bold=True, err=True)
        sys.exit(1)
    except MigrationError as e:
        click.secho(f"\nMigration Execution Error: {e}", fg="red", bold=True, err=True)
        sys.exit(1)
    except Exception as e:
        click.secho(f"\nUnexpected error: {e}", fg="red", bold=True, err=True)
        sys.exit(1)


@main.command()
@click.argument("template_path", type=click.Path(exists=True))
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to database credentials config YAML.")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose debug logging.")
def check(template_path, config, verbose):
    """Execute pre-migration and post-migration checklist assertions only (without data stream)."""
    setup_logging(verbose)
    click.secho("==================================================", fg="cyan")
    click.secho(f"Running checklists for: {template_path}", fg="cyan", bold=True)
    click.secho("==================================================", fg="cyan")

    try:
        template = MigrationTemplate(template_path)
        db_configs = load_db_config(config)
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        # Connect databases
        engine.source_db.connect()
        engine.target_db.connect()
        
        runner = ChecklistRunner(engine.source_db, engine.target_db)
        
        click.secho("\nRunning Pre-Migration Checklist...", fg="yellow")
        pre_passed, pre_results = runner.run_checklist(template.pre_migration)
        for res in pre_results:
            if res.passed:
                click.secho(f"  ✓ {res.name} (Passed)", fg="green")
            else:
                click.secho(f"  ✗ {res.name} (Failed) - {res.message}", fg="red")

        click.secho("\nRunning Post-Migration Checklist...", fg="yellow")
        post_passed, post_results = runner.run_checklist(template.post_migration)
        for res in post_results:
            if res.passed:
                click.secho(f"  ✓ {res.name} (Passed)", fg="green")
            else:
                click.secho(f"  ✗ {res.name} (Failed) - {res.message}", fg="red")
        
        engine._close_databases()
        
        if pre_passed and post_passed:
            click.secho("\n✓ All checklist assertions passed successfully.", fg="green", bold=True)
            sys.exit(0)
        else:
            click.secho("\n✗ Some checklist assertions failed.", fg="red", bold=True)
            sys.exit(1)

    except Exception as e:
        click.secho(f"\nError running checklists: {e}", fg="red", bold=True, err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
