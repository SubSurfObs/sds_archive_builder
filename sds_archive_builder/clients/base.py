"""Abstract base class for all data source clients."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from obspy import Inventory, Stream, UTCDateTime


class BaseClient(ABC):
    """
    Interface that all data clients must implement.

    All implementations must return obspy.Stream / obspy.Inventory so that
    the rest of the pipeline (SDS writer, DB tracking) is source-agnostic.
    """

    @abstractmethod
    def get_inventory(
        self,
        network: str,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
    ) -> Inventory:
        """
        Fetch station inventory for a network within the given (buffered) bounds.

        Returns an obspy.Inventory. Raises on connection failure.
        """

    @abstractmethod
    def get_waveforms(
        self,
        network: str,
        station: str,
        location: str,
        channel: str,
        day: date,
    ) -> Stream:
        """
        Fetch waveform data for a single station/channel/day.

        Returns an obspy.Stream (may be empty if no data available).
        Raises FetchError on connection or server errors.
        Raises NoDataError when the server confirms no data exists.
        """


class FetchError(Exception):
    """Raised when a data request fails due to a server or network error."""


class NoDataError(Exception):
    """Raised when a server confirms that no data exists for the request."""


class RateLimitedError(Exception):
    """Raised when the server indicates the client is rate-limited."""
    def __init__(self, message: str = "", retry_after_s: int = 60):
        super().__init__(message)
        self.retry_after_s = retry_after_s
