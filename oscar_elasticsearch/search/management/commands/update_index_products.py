import time

from django.core.management.base import BaseCommand
from oscar.core.loading import get_class, get_model
from oscar_elasticsearch.search import settings

chunked = get_class("search.utils", "chunked")
ProductElasticsearchIndex = get_class("search.api.product", "ProductElasticsearchIndex")
Product = get_model("catalogue", "Product")

class Command(BaseCommand):
    def handle(self, *args, **options):
        # Record the start time for the entire indexing process
        overall_start_time = time.time()

        products = Product.objects.browsable()
        products_count = products.count()

        # When there are no products, we should still reindex to clear the index
        if not products:
            ProductElasticsearchIndex().reindex(products)
            self.stdout.write(self.style.SUCCESS("No products found. Index cleared."))
            return

        total_chunks = (products_count + settings.INDEXING_CHUNK_SIZE - 1) // settings.INDEXING_CHUNK_SIZE
        processed_chunks = 0
        processed_products = 0

        alias_indexes = []
        for chunk in chunked(products, settings.INDEXING_CHUNK_SIZE):
            start_time = time.time()
            product_index = ProductElasticsearchIndex()
            alias_indexes.append(product_index.indexer.alias_name)
            product_index.indexer.excluded_cleanup_aliases = alias_indexes
            product_index.reindex(chunk)
            end_time = time.time()

            processed_products += len(chunk)
            processed_chunks += 1
            self.stdout.write(
                self.style.SUCCESS(
                    "Processed chunk %i/%i (products %i/%i) in %.2f seconds"
                    % (
                        processed_chunks,
                        total_chunks,
                        processed_products,
                        products_count,
                        end_time - start_time,
                    )
                )
            )

        overall_end_time = time.time()
        total_time = overall_end_time - overall_start_time

        self.stdout.write(
            self.style.SUCCESS(
                "\n%i products successfully indexed in %.2f seconds" % (processed_products, total_time)
            )
        )
