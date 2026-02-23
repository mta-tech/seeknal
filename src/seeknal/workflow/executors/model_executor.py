"""
Model executor for Seeknal workflow execution.

This module provides the ModelExecutor class for training and persisting
ML models using scikit-learn. It supports common algorithms like
LinearRegression, RandomForest, and XGBoost for both classification
and regression tasks.

Key Features:
- Parse YAML fields: training, output_columns, algorithm, params
- Load training data from resolved input refs
- Train models with scikit-learn
- Persist models as pickle files in models/ directory
- Return ExecutionResult with training metrics and timing
- Handle both classification and regression
- Support model versioning

Example:
    >>> from seeknal.workflow.executors import get_executor, ExecutionContext
    >>> from pathlib import Path
    >>>
    >>> context = ExecutionContext(
    ...     project_name="my_project",
    ...     workspace_path=Path("~/seeknal"),
    ...     target_path=Path("target"),
    ... )
    >>> executor = get_executor(node, context)
    >>> result = executor.run()
    >>> print(f"Trained model with accuracy: {result.metadata['accuracy']}")
"""

from __future__ import annotations

import pickle
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutionContext,
    ExecutionStatus,
    ExecutorResult,
    ExecutorValidationError,
    ExecutorExecutionError,
    register_executor,
)


# Supported algorithms mapping
ALGORITHM_REGISTRY: Dict[str, Dict[str, Any]] = {
    "linear_regression": {
        "class": "sklearn.linear_model.LinearRegression",
        "type": "regression",
        "default_params": {},
    },
    "logistic_regression": {
        "class": "sklearn.linear_model.LogisticRegression",
        "type": "classification",
        "default_params": {
            "max_iter": 1000,
            "random_state": 42,
        },
    },
    "random_forest": {
        "class": "sklearn.ensemble.RandomForestClassifier",
        "type": "classification",
        "default_params": {
            "n_estimators": 100,
            "max_depth": 6,
            "random_state": 42,
        },
    },
    "random_forest_regressor": {
        "class": "sklearn.ensemble.RandomForestRegressor",
        "type": "regression",
        "default_params": {
            "n_estimators": 100,
            "max_depth": 6,
            "random_state": 42,
        },
    },
    "xgboost": {
        "class": "xgboost.XGBClassifier",
        "type": "classification",
        "default_params": {
            "n_estimators": 100,
            "max_depth": 6,
            "learning_rate": 0.1,
            "random_state": 42,
        },
    },
    "xgboost_regressor": {
        "class": "xgboost.XGBRegressor",
        "type": "regression",
        "default_params": {
            "n_estimators": 100,
            "max_depth": 6,
            "learning_rate": 0.1,
            "random_state": 42,
        },
    },
    "svm": {
        "class": "sklearn.svm.SVC",
        "type": "classification",
        "default_params": {
            "kernel": "rbf",
            "random_state": 42,
        },
    },
    "svm_regressor": {
        "class": "sklearn.svm.SVR",
        "type": "regression",
        "default_params": {
            "kernel": "rbf",
        },
    },
}


@register_executor(NodeType.MODEL)
class ModelExecutor(BaseExecutor):
    """
    Executor for MODEL nodes.

    Handles ML model training, persistence, and metadata tracking.
    Supports scikit-learn and XGBoost algorithms for both
    classification and regression tasks.

    YAML Structure:
        kind: model
        name: my_model
        output_columns:
          - prediction
          - probability
        inputs:
          - ref: source.features
        training:
          label_source: source.labels
          label_column: target
          algorithm: xgboost
          params:
            n_estimators: 100
            max_depth: 6

    Attributes:
        node: The model node being executed
        context: The execution context (paths, databases, etc.)

    Example:
        >>> executor = ModelExecutor(node, context)
        >>> result = executor.run()
        >>> if result.is_success():
        ...     print(f"Model saved to: {result.output_path}")
    """

    @property
    def node_type(self) -> NodeType:
        """Return the node type this executor handles."""
        return NodeType.MODEL

    def validate(self) -> None:
        """
        Validate the model node configuration.

        Checks for:
        - Required fields: output_columns, inputs, training
        - Valid algorithm specification
        - Valid label source reference

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        config = self.node.config

        # Check output_columns
        output_columns = config.get("output_columns")
        if not output_columns or not isinstance(output_columns, list):
            raise ExecutorValidationError(
                self.node.id,
                "Missing or invalid 'output_columns' field (must be a list)"
            )

        if len(output_columns) == 0:
            raise ExecutorValidationError(
                self.node.id,
                "'output_columns' cannot be empty"
            )

        # Check inputs
        inputs = config.get("inputs")
        if not inputs or not isinstance(inputs, list):
            raise ExecutorValidationError(
                self.node.id,
                "Missing or invalid 'inputs' field (must be a list)"
            )

        if len(inputs) == 0:
            raise ExecutorValidationError(
                self.node.id,
                "'inputs' cannot be empty - must specify at least one data source"
            )

        # Check training configuration
        training = config.get("training")
        if not training or not isinstance(training, dict):
            raise ExecutorValidationError(
                self.node.id,
                "Missing or invalid 'training' field (must be a dict)"
            )

        # Validate algorithm
        algorithm = training.get("algorithm")
        if not algorithm:
            raise ExecutorValidationError(
                self.node.id,
                "Missing 'algorithm' in training configuration"
            )

        if algorithm not in ALGORITHM_REGISTRY:
            available = ", ".join(sorted(ALGORITHM_REGISTRY.keys()))
            raise ExecutorValidationError(
                self.node.id,
                f"Unsupported algorithm '{algorithm}'. Available: {available}"
            )

        # Validate label source
        label_source = training.get("label_source")
        if not label_source:
            raise ExecutorValidationError(
                self.node.id,
                "Missing 'label_source' in training configuration"
            )

        label_column = training.get("label_column")
        if not label_column:
            raise ExecutorValidationError(
                self.node.id,
                "Missing 'label_column' in training configuration"
            )

    def pre_execute(self) -> None:
        """
        Prepare execution environment.

        Creates the models/ directory if it doesn't exist.
        """
        models_dir = self.context.target_path / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

    def execute(self) -> ExecutorResult:
        """
        Execute the model training.

        Steps:
        1. Load training data from input refs
        2. Extract features and labels
        3. Initialize model with specified algorithm and params
        4. Train the model
        5. Save model to disk
        6. Calculate training metrics
        7. Return ExecutorResult with metadata

        Returns:
            ExecutorResult with training outcome

        Raises:
            ExecutorExecutionError: If training fails
        """
        start_time = time.time()

        # Handle dry-run mode
        if self.context.dry_run:
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=0.0,
                row_count=0,
                metadata={
                    "dry_run": True,
                    "algorithm": self.node.config.get("training", {}).get("algorithm"),
                    "output_columns": self.node.config.get("output_columns", []),
                },
                is_dry_run=True,
            )

        try:
            # Step 1: Load training data
            X_train, y_train, feature_names = self._load_training_data()

            # Step 2: Get algorithm configuration
            algorithm_config = self._get_algorithm_config()

            # Step 3: Initialize model
            model = self._create_model(algorithm_config)

            # Step 4: Train model
            from seeknal.cli.main import _echo_info
            _echo_info(f"Training {algorithm_config['type']} model: {algorithm_config['algorithm']}")
            model.fit(X_train, y_train)

            # Step 5: Save model
            model_path = self._save_model(model, algorithm_config)

            # Step 6: Calculate metrics
            metrics = self._calculate_metrics(
                model, X_train, y_train, algorithm_config["type"]
            )

            duration = time.time() - start_time

            # Step 7: Return result
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=len(X_train),
                output_path=model_path,
                metadata={
                    "algorithm": algorithm_config["algorithm"],
                    "model_type": algorithm_config["type"],
                    "feature_count": len(feature_names),
                    "training_samples": len(X_train),
                    "model_path": str(model_path),
                    **metrics,
                },
            )

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Model training failed: {str(e)}",
                e,
            ) from e

    def _load_training_data(self) -> tuple[pd.DataFrame, pd.Series, List[str]]:
        """
        Load training data from resolved input refs.

        Returns:
            Tuple of (X_train, y_train, feature_names)

        Raises:
            ExecutorExecutionError: If data loading fails
        """
        config = self.node.config
        inputs = config.get("inputs", [])
        training = config.get("training", {})

        # For now, we'll load from the first input ref
        # In production, this would resolve refs via the manifest/state
        input_ref = inputs[0].get("ref", "")
        if not input_ref:
            raise ExecutorExecutionError(
                self.node.id,
                "First input must specify 'ref'"
            )

        # Try to load from cache/target directory
        # This assumes data has been materialized by upstream nodes
        try:
            # Extract node name from ref (e.g., "source.features" -> "source")
            ref_parts = input_ref.split(".")
            source_name = ref_parts[0]

            # Try to find cached data
            cache_path = self.context.state_dir / f"{source_name}.parquet"

            if cache_path.exists():
                df = pd.read_parquet(cache_path)
            else:
                # Fallback: try to load from target directory
                data_path = self.context.target_path / "source" / f"{source_name}.parquet"
                if not data_path.exists():
                    raise ExecutorExecutionError(
                        self.node.id,
                        f"Cannot find training data for ref '{input_ref}'. "
                        f"Looked in: {cache_path}, {data_path}"
                    )
                df = pd.read_parquet(data_path)

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load training data from '{input_ref}': {str(e)}",
                e,
            ) from e

        # Extract features and labels
        label_column = training.get("label_column", "target")

        if label_column not in df.columns:
            raise ExecutorExecutionError(
                self.node.id,
                f"Label column '{label_column}' not found in data. "
                f"Available columns: {list(df.columns)}"
            )

        # Separate features and target
        y_train = df[label_column]
        X_train = df.drop(columns=[label_column])
        feature_names = X_train.columns.tolist()

        # Convert to numpy arrays for sklearn
        X_train = X_train.values
        y_train = y_train.values

        return X_train, y_train, feature_names

    def _get_algorithm_config(self) -> Dict[str, Any]:
        """
        Get algorithm configuration from node config.

        Returns:
            Dict with algorithm info including class, type, params

        Raises:
            ExecutorExecutionError: If algorithm not found
        """
        training = self.node.config.get("training", {})
        algorithm = training.get("algorithm", "")

        if algorithm not in ALGORITHM_REGISTRY:
            raise ExecutorExecutionError(
                self.node.id,
                f"Unsupported algorithm: {algorithm}"
            )

        algo_info = ALGORITHM_REGISTRY[algorithm].copy()
        algo_info["algorithm"] = algorithm

        # Merge with user-provided params
        user_params = training.get("params", {})
        default_params = algo_info.pop("default_params", {})
        algo_info["params"] = {**default_params, **user_params}

        return algo_info

    def _create_model(self, algorithm_config: Dict[str, Any]) -> Any:
        """
        Create model instance from algorithm configuration.

        Args:
            algorithm_config: Algorithm config from _get_algorithm_config()

        Returns:
            Initialized model instance

        Raises:
            ExecutorExecutionError: If model creation fails
        """
        try:
            # Import the model class
            class_path = algorithm_config["class"]
            module_path, class_name = class_path.rsplit(".", 1)

            if class_path.startswith("sklearn."):
                import importlib
                module = importlib.import_module(module_path)
                model_class = getattr(module, class_name)
            elif class_path.startswith("xgboost."):
                import xgboost
                model_class = xgboost.XGBClassifier if "Classifier" in class_name else xgboost.XGBRegressor
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unsupported module: {module_path}"
                )

            # Instantiate with params
            params = algorithm_config.get("params", {})
            model = model_class(**params)

            return model

        except ImportError as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to import model class '{class_path}': {str(e)}. "
                f"Ensure required packages are installed (scikit-learn, xgboost, etc.)",
                e,
            ) from e
        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to create model: {str(e)}",
                e,
            ) from e

    def _save_model(self, model: Any, algorithm_config: Dict[str, Any]) -> Path:
        """
        Save trained model to disk.

        Args:
            model: Trained model instance
            algorithm_config: Algorithm configuration

        Returns:
            Path to saved model file
        """
        models_dir = self.context.target_path / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        # Create filename: {model_name}_{algorithm}_{timestamp}.pkl
        timestamp = int(time.time())
        filename = f"{self.node.name}_{algorithm_config['algorithm']}_{timestamp}.pkl"
        model_path = models_dir / filename

        # Save model with metadata
        model_data = {
            "model": model,
            "algorithm": algorithm_config["algorithm"],
            "model_type": algorithm_config["type"],
            "node_id": self.node.id,
            "node_name": self.node.name,
            "trained_at": time.time(),
            "params": algorithm_config.get("params", {}),
        }

        with open(model_path, "wb") as f:
            pickle.dump(model_data, f)

        return model_path

    def _calculate_metrics(
        self,
        model: Any,
        X: pd.DataFrame,
        y: pd.Series,
        model_type: str,
    ) -> Dict[str, float]:
        """
        Calculate training metrics.

        Args:
            model: Trained model
            X: Features
            y: True labels
            model_type: 'classification' or 'regression'

        Returns:
            Dict of metric names to values
        """
        try:
            # Get predictions
            y_pred = model.predict(X)

            metrics: Dict[str, float] = {}

            if model_type == "classification":
                from sklearn.metrics import (
                    accuracy_score,
                    precision_score,
                    recall_score,
                    f1_score,
                )

                metrics["accuracy"] = float(accuracy_score(y, y_pred))

                # Try to calculate precision/recall (may fail for binary/multiclass issues)
                try:
                    metrics["precision"] = float(precision_score(y, y_pred, average="weighted"))
                    metrics["recall"] = float(recall_score(y, y_pred, average="weighted"))
                    metrics["f1_score"] = float(f1_score(y, y_pred, average="weighted"))
                except Exception:
                    pass

                # Try to get class probabilities if available
                if hasattr(model, "predict_proba"):
                    try:
                        y_proba = model.predict_proba(X)
                        metrics["has_probabilities"] = True
                    except Exception:
                        metrics["has_probabilities"] = False
                else:
                    metrics["has_probabilities"] = False

            elif model_type == "regression":
                from sklearn.metrics import (
                    mean_squared_error,
                    mean_absolute_error,
                    r2_score,
                )

                metrics["mse"] = float(mean_squared_error(y, y_pred))
                metrics["rmse"] = float(metrics["mse"] ** 0.5)
                metrics["mae"] = float(mean_absolute_error(y, y_pred))
                metrics["r2_score"] = float(r2_score(y, y_pred))

            else:
                metrics["metrics_available"] = False

            return metrics

        except Exception as e:
            # Return basic metrics if calculation fails
            return {
                "metrics_available": False,
                "error": str(e),
            }

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Post-execution cleanup and metadata enrichment.

        Args:
            result: Result from execute()

        Returns:
            Enhanced ExecutorResult
        """
        # Add executor version
        result.metadata["executor_version"] = "1.0.0"

        # Add model storage location
        if result.output_path:
            result.metadata["model_storage_path"] = str(result.output_path)

        return result
