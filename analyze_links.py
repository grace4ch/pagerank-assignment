import os
import numpy as np
from google.cloud import storage

# Function to download files from the bucket
def download_files_from_bucket(bucket_name, local_directory):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Create local directory 
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    page_links = {}
    total_files = 0
    blobs = bucket.list_blobs()  # Retrieve all files from the bucket

    for blob in blobs:  # Loop through all blobs
        blob_name = blob.name
        print(f"Attempting to download: {blob_name}")
        local_file_path = os.path.join(local_directory, blob_name)
        try:
            blob.download_to_filename(local_file_path)
            total_files += 1
            print(f"Downloaded {total_files} files...")

            # Process the file and add to page_links dictionary
            with open(local_file_path, 'r') as f:
                links = [line.strip() for line in f.readlines()]
                page_links[blob_name] = links
        except Exception as e:
            print(f"Error downloading {blob_name}: {e}")
    
    print(f"Downloaded {total_files} files in total.")
    return page_links

# Function to compute statistics
def compute_statistics(page_links):
    outgoing_counts = [len(links) for links in page_links.values()]
    incoming_counts = [sum(1 for links in page_links.values() if page in links) for page in page_links.keys()]

    outgoing_stats = {
        'average': np.mean(outgoing_counts),
        'median': np.median(outgoing_counts),
        'min': np.min(outgoing_counts),
        'max': np.max(outgoing_counts),
        'quintiles': np.percentile(outgoing_counts, [20, 40, 60, 80])
    }

    incoming_stats = {
        'average': np.mean(incoming_counts),
        'median': np.median(incoming_counts),
        'min': np.min(incoming_counts),
        'max': np.max(incoming_counts),
        'quintiles': np.percentile(incoming_counts, [20, 40, 60, 80])
    }

    return outgoing_stats, incoming_stats

# Function to calculate PageRank
def calculate_pagerank(page_links, damping_factor=0.85, max_iterations=100, tol=1e-6):
    num_pages = len(page_links)
    pageranks = {page: 1 / num_pages for page in page_links}

    for iteration in range(max_iterations):
        new_pageranks = {}
        for page in page_links:
            incoming_pages = [p for p, links in page_links.items() if page in links]
            rank_sum = sum(pageranks[incoming_page] / len(page_links[incoming_page]) for incoming_page in incoming_pages)
            new_pageranks[page] = (1 - damping_factor) / num_pages + damping_factor * rank_sum

        # Check for convergence
        diff = sum(abs(new_pageranks[page] - pageranks[page]) for page in pageranks)
        pageranks = new_pageranks
        print(f"Iteration {iteration}, total difference: {diff}")

        if diff < tol:
            print(f"Converged after {iteration} iterations")
            break

    return sorted(pageranks.items(), key=lambda x: x[1], reverse=True)[:5]

if __name__ == "__main__":
    
    bucket_name = 'gracegcb-east1'
    local_directory = 'downloaded_pages'

    # Step 1: Download the files from the bucket
    page_links = download_files_from_bucket(bucket_name, local_directory)

    # Step 2: Compute statistics
    outgoing_stats, incoming_stats = compute_statistics(page_links)
    print("Outgoing Links Statistics:", outgoing_stats)
    print("Incoming Links Statistics:", incoming_stats)

    # Step 3: Calculate PageRank and output the top 5 pages
    pageranks = calculate_pagerank(page_links)
    print("Top 5 Pages by PageRank:", pageranks)

