import json
import yaml

from typing import List, Dict, Tuple, Any, Union, cast

from rich.console import Console
from rich.table import Table

from armonik_cli.utils import CLIJSONEncoder


class ArmoniKCLIConsole(Console):
    def formatted_print(
        self, obj: object, format: str, table_cols: Union[List[Tuple[str, str]], None] = None
    ):
        obj = cast(Dict[str, Any], json.loads(json.dumps(obj, cls=CLIJSONEncoder)))

        if format == "yaml":
            obj = yaml.dump(obj, sort_keys=False, indent=2)
        elif format == "table":
            if not table_cols:
                raise ValueError("Missing 'table_cols' when calling 'formatted_print' with format table.")
            obj = self._build_table(obj, table_cols)
        else:
            obj = json.dumps(obj, sort_keys=False, indent=2)

        super().print(obj)

    @staticmethod
    def _build_table(obj: Dict[str, Any], table_cols: List[Tuple[str, str]]) -> Table:
        table = Table(box=None)

        for col_name, _ in table_cols:
            table.add_column(col_name)

        objs = obj if isinstance(obj, List) else [obj]
        for item in objs:
            table.add_row(*[item[key] for _, key in table_cols])

        return table


console = ArmoniKCLIConsole()
