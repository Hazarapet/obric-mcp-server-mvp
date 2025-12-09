"""Embedding generation using langchain and OpenAI.

This module provides the EmbeddingClient class for generating embeddings
from text using OpenAI's embedding models.
"""

import logging
from typing import List, Optional

from langchain_openai import OpenAIEmbeddings

from ..config import Config

logger = logging.getLogger(__name__)


class EmbeddingClient:
    """Generic LangChain embedding client wrapper for text embedding extraction."""

    def __init__(self, config: Config):
        """Initialize embedding client with configuration.

        Args:
            config: Application configuration instance.
        """
        self.config = config
        self._embeddings: Optional[OpenAIEmbeddings] = None

    @property
    def embeddings(self) -> OpenAIEmbeddings:
        """Get or create the embeddings instance.

        Returns:
            OpenAIEmbeddings instance, lazily initialized.
        """
        if self._embeddings is None:
            if self.config.embedding_dimensions is not None:
                self._embeddings = OpenAIEmbeddings(
                    model=self.config.embedding_model_name,
                    api_key=self.config.openai_api_key,
                    dimensions=self.config.embedding_dimensions,
                )
            else:
                self._embeddings = OpenAIEmbeddings(
                    model=self.config.embedding_model_name,
                    api_key=self.config.openai_api_key,
                )
        return self._embeddings

    def embed_text(self, text: str) -> List[float]:
        """Generate embedding for a single text string.

        Args:
            text: Text to embed

        Returns:
            List of float values representing the embedding vector

        Raises:
            Exception: If embedding extraction fails.
        """
        try:
            # Use embed_documents for both single and batch (it supports both)
            results = self.embeddings.embed_documents([text])
            return results[0] if results else []
        except Exception as e:
            logger.error(f"Embedding extraction failed for text: {e}", exc_info=True)
            raise

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of text strings.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors (each vector is a list of floats)

        Raises:
            Exception: If batch embedding extraction fails.
        """
        if not texts:
            return []

        try:
            results = self.embeddings.embed_documents(texts)
            return results
        except Exception as e:
            logger.error(f"Batch embedding extraction failed: {e}", exc_info=True)
            raise

