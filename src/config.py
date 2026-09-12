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

    # Configuracao do MinIO
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "minio"
    minio_secret_key: str = "minio123"
    minio_bucket: str = "stream-pipeline"

    @property
    def output_dir(self) -> Path:
        return self.base_dir / "data" / "output"

    @property
    def input_dir(self) -> Path:
        return self.base_dir / "data" / "input"

    # Caminhos locais mantidos para compatibilidade
    # e para arquivos auxiliares da execucao.
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

    # ==========================
    # Caminhos no MinIO
    # ==========================

    @property
    def raw_storage_path(self) -> str:
        return f"s3a://{self.minio_bucket}/raw"

    @property
    def trusted_storage_path(self) -> str:
        return f"s3a://{self.minio_bucket}/trusted"

    @property
    def refined_storage_path(self) -> str:
        return f"s3a://{self.minio_bucket}/refined"

    @property
    def raw_storage_checkpoint(self) -> str:
        return f"s3a://{self.minio_bucket}/checkpoints/raw"

    @property
    def trusted_storage_checkpoint(self) -> str:
        return f"s3a://{self.minio_bucket}/checkpoints/trusted"

    @property
    def refined_storage_checkpoint(self) -> str:
        return f"s3a://{self.minio_bucket}/checkpoints/refined"

    @classmethod
    def from_environment(cls) -> "PipelineConfig":
        """Carrega parametros opcionais do ambiente para execucao em deploy."""

        return cls(
            base_dir=Path(
                os.getenv(
                    "PIPELINE_BASE_DIR",
                    str(cls.base_dir)
                )
            ),

            rows_per_second=int(
                os.getenv("PIPELINE_ROWS_PER_SECOND", "5")
            ),

            window_duration=os.getenv(
                "PIPELINE_WINDOW",
                "10 seconds"
            ),

            watermark_duration=os.getenv(
                "PIPELINE_WATERMARK",
                "30 seconds"
            ),

            trigger_interval=os.getenv(
                "PIPELINE_TRIGGER",
                "5 seconds"
            ),

            runtime_seconds=int(
                os.getenv(
                    "PIPELINE_RUNTIME_SECONDS",
                    "30"
                )
            ),

            minio_endpoint=os.getenv(
                "MINIO_ENDPOINT", "http://172.18.0.1:9000"
            ),

            minio_access_key=os.getenv(
                "MINIO_ACCESS_KEY",
                "minio"
            ),

            minio_secret_key=os.getenv(
                "MINIO_SECRET_KEY",
                "minio123"
            ),

            minio_bucket=os.getenv(
                "MINIO_BUCKET",
                "stream-pipeline"
            ),
        )