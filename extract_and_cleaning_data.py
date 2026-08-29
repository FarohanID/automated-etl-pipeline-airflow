import requests

query = "Data Engineering"
url =  f"https://openlibrary.org/search.json?q={query.replace(' ', '+')}"

response = requests.get(url)
data = response.json()

# print(data)
books = []

for book in data['docs'][:10]:
    # print(book.get('title'))
    # # print(book.get('author_name'))
    # print(book.get('author_name')[0] if book.get('author_name') else "Author not available")
    # print(book.get('first_publish_year'))
    # print("================================")
    books.append({
        'title': book.get('title'),
        'author_name': book.get('author_name'),
        'first_publish_year': book.get('first_publish_year')
    })

for book in books:
    print(f"Title: {book['title']}")
    print(f"Author: {book['author_name'][0] if book['author_name'] else 'Author not available'}")
    print(f"First Publish Year: {book['first_publish_year']}")
    print("================================")