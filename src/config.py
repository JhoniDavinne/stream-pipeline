"""Configuracoes do pipeline e dos caminhos de entrada, saida e checkpoint."""

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class PipelineConfig:
    """Parametros do processamento, sobrescritos por variaveis de ambiente."""

    base_dir: Path = Path(__file__).resolve().parents[1]
    rows_per_second: int = 5
    window_duration: str = "10 seconds"
    watermark_duration: str = "30 seconds"
    trigger_interval: str = "5 seconds"
    runtime_seconds: int = 30

    @property
    def output_dir(self) -> Path:
        return self.base_dir / "data" / "output"

    @property
    def input_dir(self) -> Path:
        return self.base_dir / "data" / "input"

    @property
    def raw_dir(self) -> Path:
        return self.base_dir / "data" / "raw"

    @property
    def trusted_dir(self) -> Path:
        return self.base_dir / "data" / "trusted"

    @property
    def refined_dir(self) -> Path:
        return self.base_dir / "data" / "refined"

    @property
    def sqlite_path(self) -> Path:
        return self.refined_dir / "vehicle_financing.sqlite"

    @property
    def checkpoint_dir(self) -> Path:
        return self.base_dir / "data" / "checkpoint"

    @property
    def raw_checkpoint_dir(self) -> Path:
        return self.checkpoint_dir / "raw"

    @property
    def trusted_checkpoint_dir(self) -> Path:
        return self.checkpoint_dir / "trusted"

    @property
    def refined_checkpoint_dir(self) -> Path:
        return self.checkpoint_dir / "refined"

    @classmethod
    def from_environment(cls) -> "PipelineConfig":
        """Carrega parametros opcionais do ambiente para execucao em deploy."""
        return cls(
            base_dir=Path(os.getenv("PIPELINE_BASE_DIR", str(cls.base_dir))),
            rows_per_second=int(os.getenv("PIPELINE_ROWS_PER_SECOND", "5")),
            window_duration=os.getenv("PIPELINE_WINDOW", "10 seconds"),
            watermark_duration=os.getenv("PIPELINE_WATERMARK", "30 seconds"),
            trigger_interval=os.getenv("PIPELINE_TRIGGER", "5 seconds"),
            runtime_seconds=int(os.getenv("PIPELINE_RUNTIME_SECONDS", "30")),
        )
