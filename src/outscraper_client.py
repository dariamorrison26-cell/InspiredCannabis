"""Outscraper API client for fetching Google Maps reviews."""

import logging
import time
from datetime import date, datetime
from typing import Optional

from outscraper import ApiClient

logger = logging.getLogger(__name__)


class OutscraperClient:
    """Wrapper around Outscraper's Google Maps Reviews API."""

    def __init__(self, api_key: str):
        self.client = ApiClient(api_key=api_key)

    def fetch_reviews(
        self,
        place_ids: list[str],
        cutoff_date: Optional[date] = None,
        reviews_limit: int = 200,
        batch_size: int = 10
    ) -> list[dict]:
        """
        Fetch reviews for a list of place IDs.

        Args:
            place_ids: List of Google Maps Place IDs
            cutoff_date: Only return reviews after this date (for incremental)
            reviews_limit: Max reviews per place (0 = all)
            batch_size: How many places to query per API call

        Returns:
            List of normalized review dicts
        """
        all_reviews = []

        for i in range(0, len(place_ids), batch_size):
            batch = place_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(place_ids) + batch_size - 1) // batch_size

            logger.info(f"Fetching reviews batch {batch_num}/{total_batches} ({len(batch)} places)")

            try:
                results = self.client.google_maps_reviews(
                    batch,
                    reviews_limit=reviews_limit,
                    language="en",
                    sort="newest",
                    cutoff=int(datetime.combine(cutoff_date, datetime.min.time()).timestamp()) if cutoff_date else None
                )

                for place_data in results:
                    if not isinstance(place_data, dict):
                        continue

                    place_id = place_data.get("place_id", "")
                    current_rating = place_data.get("rating")
                    reviews_data = place_data.get("reviews_data", [])

                    if not reviews_data:
                        logger.info(f"No reviews found for {place_id}")
                        continue

                    for review in reviews_data:
                        normalized = self._normalize_review(review, place_id)
                        if normalized:
                            # Apply cutoff filter (Outscraper may not filter exactly)
                            if cutoff_date and normalized["review_date"]:
                                try:
                                    review_dt = date.fromisoformat(normalized["review_date"])
                                    if review_dt < cutoff_date:
                                        continue
                                except ValueError:
                                    pass

                            all_reviews.append(normalized)

                    # Store the current rating alongside reviews
                    if current_rating is not None:
                        all_reviews.append({
                            "_type": "store_rating",
                            "place_id": place_id,
                            "current_rating": current_rating
                        })

                    logger.info(
                        f"  {place_id}: {len(reviews_data)} reviews fetched, "
                        f"current rating: {current_rating}"
                    )

            except Exception as e:
                logger.error(f"Error fetching batch {batch_num}: {e}")
                raise

            # Rate limiting: pause between batches
            if i + batch_size < len(place_ids):
                time.sleep(2)

        review_count = sum(1 for r in all_reviews if r.get("_type") != "store_rating")
        logger.info(f"Total reviews fetched: {review_count}")
        return all_reviews

    def _normalize_review(self, review: dict, place_id: str) -> Optional[dict]:
        """Normalize an Outscraper review into our standard format."""
        try:
            # Outscraper returns review_datetime_utc or review_timestamp
            review_date_str = review.get("review_datetime_utc", "")
            if review_date_str:
                # Parse "MM/DD/YYYY HH:MM:SS" or ISO format
                try:
                    from datetime import datetime
                    if "/" in review_date_str:
                        dt = datetime.strptime(review_date_str, "%m/%d/%Y %H:%M:%S")
                    else:
                        dt = datetime.fromisoformat(review_date_str.replace("Z", "+00:00"))
                    review_date_str = dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    review_date_str = ""

            return {
                "place_id": place_id,
                "rating": int(review.get("review_rating", 0)),
                "review_date": review_date_str,
                "reviewer_name": review.get("author_title", ""),
                "review_text": review.get("review_text", ""),
                "owner_response": review.get("owner_answer", None)
            }
        except Exception as e:
            logger.warning(f"Failed to normalize review: {e}")
            return None

    def test_connection(self, place_id: str) -> dict:
        """Test API connection by fetching 1 review from a single place."""
        logger.info(f"Testing API connection with place_id: {place_id}")
        results = self.client.google_maps_reviews(
            [place_id],
            reviews_limit=1,
            language="en"
        )
        if results and isinstance(results[0], dict):
            return {
                "success": True,
                "place_name": results[0].get("name", "Unknown"),
                "rating": results[0].get("rating"),
                "total_reviews": results[0].get("reviews", 0),
                "sample_review": results[0].get("reviews_data", [{}])[0] if results[0].get("reviews_data") else None
            }
        return {"success": False, "error": "No data returned"}
