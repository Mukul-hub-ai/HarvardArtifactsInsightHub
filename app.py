import streamlit as st
import requests
import pandas as pd
import pymysql

#connection
def get_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='project_1',
        port=3306,
        cursorclass=pymysql.cursors.DictCursor
    )

#API Data Fetching
def fetch_data_api(classification, pages=3):
    API_KEY = "9ca1111c-6a05-45dc-817e-7bab5939072a"
    base_url = "https://api.harvardartmuseums.org/object"
    all_records = []
    for page in range(1, pages+1):
        params = {
            "apikey": API_KEY,
            "classification": classification,
            "size": 100,
            "page": page
        }
        response = requests.get(base_url, params=params)
        data = response.json()
        if "records" in data:
            all_records.extend(data["records"])
        else:
            break
    return all_records

# Data Processing
def process_metadata(records):
    metadata_list = []
    for r in records:
        metadata_list.append({
            "id": r.get("id"),
            "title": r.get("title", ""),
            "culture": r.get("culture", ""),
            "period": r.get("period", ""),
            "century": r.get("century", ""),
            "medium": r.get("medium", ""),
            "dimensions": r.get("dimensions", ""),
            "description": r.get("description", ""),
            "department": r.get("department", ""),
            "classification": r.get("classification", ""),
            "accessionyear": r.get("accessionyear", None),
            "accessionmethod": r.get("accessionmethod", "")
        })
    return pd.DataFrame(metadata_list)

def process_media(records):
    media_list = []
    for r in records:
        media_list.append({
            "objid": r.get("id", None),
            "imagecount": r.get("imagecount", None),
            "mediacount": r.get("mediacount", None),
            "colorcount": r.get("colorcount", None),
            "rank": r.get("rank", None),
            "datebegin": r.get("datebegin", None),
            "dateend": r.get("dateend", None)
        })
    return pd.DataFrame(media_list)

def process_colors(records):
    colors_list = []
    for r in records:
        colors = r.get("colors", [])
        for c in colors:
            colors_list.append({
                "objid": r.get("id", None),
                "color": c.get("color", ""),
                "spectrum": c.get("spectrum", ""),
                "hue": c.get("hue", ""),
                "percent": c.get("percent", None),
                "css3": c.get("css3", "")
            })
    return pd.DataFrame(colors_list)

#SQL Table Creation
def create_tables():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS metadata (
        id INT PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        dimensions TEXT,
        description TEXT,
        department TEXT,
        classification TEXT,
        accessionyear INT,
        accessionmethod TEXT
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS media (
        objid INT,
        imagecount INT,
        mediacount INT,
        colorcount INT,
        `rank` INT,
        datebegin INT,
        dateend INT
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS colors (
        objid INT,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent FLOAT,
        css3 TEXT
    )""")
    conn.commit()
    cursor.close()
    conn.close()

# Data Insert Functions
def insert_metadata(df):
    conn = get_connection()
    cursor = conn.cursor()
    sql = """INSERT IGNORE INTO metadata VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql, df.fillna('').values.tolist())
    conn.commit()
    cursor.close()
    conn.close()

def insert_media(df):
    conn = get_connection()
    cursor = conn.cursor()
    sql = """INSERT IGNORE INTO media VALUES(%s,%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql, df.fillna(0).values.tolist())
    conn.commit()
    cursor.close()
    conn.close()

def insert_colors(df):
    conn = get_connection()
    cursor = conn.cursor()
    sql = """INSERT IGNORE INTO colors VALUES(%s,%s,%s,%s,%s,%s)"""
    cursor.executemany(sql, df.fillna('').values.tolist())
    conn.commit()
    cursor.close()
    conn.close()

#Query Runner
def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    df = pd.DataFrame(result, columns=columns)
    return df


def fetch_metadata_sample():
    query = "SELECT * FROM metadata LIMIT 10;"
    return run_query(query)

def fetch_media_sample():
    query = "SELECT * FROM media LIMIT 10;"
    return run_query(query)

def fetch_colors_sample():
    query = "SELECT * FROM colors LIMIT 10;"
    return run_query(query)


#Streamlit Interface
def main():
    st.title("Harvard Artifacts Data Explorer")

    menu = ["Home", "Fetch Data from API", "Insert Data to SQL", "Run Sample Queries"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Home":
        st.write("Welcome! Use the menu to fetch data, save into SQL, or run queries.")

    elif choice == "Fetch Data from API":
        classification = st.text_input("Enter Classification (e.g., Paintings, Coins, Sculptures):")
        pages = st.number_input("Pages to fetch (default 3):", value=3, min_value=1, max_value=5, step=1)
        if st.button("Fetch Data"):
            with st.spinner("Fetching data..."):
                records = fetch_data_api(classification, pages)
                st.write(f"{len(records)} records fetched.")
                df_meta = process_metadata(records)
                st.dataframe(df_meta.head())

                df_meta.to_csv('metadata.csv', index=False)
                # Saving media and colors as well for later
                df_media = process_media(records)
                df_colors = process_colors(records)
                df_media.to_csv('media.csv', index=False)
                df_colors.to_csv('colors.csv', index=False)
                st.success("Data saved locally as CSV files.")

    elif choice == "Insert Data to SQL":
        create_tables()
        try:
            df_meta = pd.read_csv('metadata.csv')
            df_media = pd.read_csv('media.csv')
            df_colors = pd.read_csv('colors.csv')

            if st.button("Insert"):
                with st.spinner('Inserting data into SQL...'):
                    insert_metadata(df_meta)
                    insert_media(df_media)
                    insert_colors(df_colors)
                    st.success("Data inserted into MySQL successfully!")
                    
                # Helper functions use karke 3 tables ke sample data show karo
                st.subheader("Sample data from metadata table:")
                st.dataframe(fetch_metadata_sample())

                st.subheader("Sample data from media table:")
                st.dataframe(fetch_media_sample())

                st.subheader("Sample data from colors table:")
                st.dataframe(fetch_colors_sample())
        except FileNotFoundError:
            st.warning("CSV files not found! Please fetch data from API first.")

    elif choice == "Run Sample Queries":
        st.subheader("Select and Run a Query")
        queries = {
            "1.List all artifacts from the 11th century belonging to Byzantine culture.": """
                select century,culture from metadata 
            where culture = 'Byzantine' and century = '11th century';
            """,
            
            "2.What are the unique cultures represented in the artifacts?": """
                select DISTINCT culture from metadata;
            """,

             "3.List all artifacts from the Archaic Period.": """
                select * from metadata where period = 'Archaic Period';
            """,

             "4.List artifact titles ordered by accession year in descending order": """
                SELECT title, accessionyear from metadata
            where accessionyear is not null 
              order by accessionyear DESC;
            """,

             "5.How many artifacts are there per department?": """
                select department, count(*) as artifact_total
             from metadata 
             group by department
             order by artifact_total;
            """,

             "6.Which artifacts have more than 1 image?": """
                select * from media
              where imagecount > 1;
            """,

             "7.What is the average rank of all artifacts?": """
                select avg(`rank`) as average_rank
              from media;
            """,

             "8.Which artifacts have a higher colorcount than mediacount?": """
                SELECT * FROM media
              WHERE colorcount > mediacount;
            """,

             "9.List all artifacts created between 1500 and 1600.": """
                select * from media 
              where datebegin >= 1500 and dateend <= 1600;
            """,

             "10.How many artifacts have no media files?": """
                select count(*) as no_media
              from media
              where mediacount = 0;
            """,

             "11.What are all the distinct hues used in the dataset?": """
                select distinct hue 
               from colors;
            """,

             "12.What are the top 5 most used colors by frequency?": """
                select color, count(*) as frequency
              from colors
              group by color
              order by frequency desc 
              limit 5;
            """,

             "13.What is the average coverage percentage for each hue?": """
                select hue, avg(percent) as coverage_average
               from colors
               group by hue;
            """,

             "14.List all colors used for a given artifact ID.": """
                select * from colors
              where objid = 280069;
            """,

             "15.What is the total number of color entries in the dataset?": """
                select count(*) as total_entries
              from colors;
            """,

             "16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.": """
                SELECT m.title, c.hue
              FROM metadata m
              INNER JOIN colors c ON m.id = c.objid
              WHERE m.culture = 'Byzantine';
            """,

            
             "17.List each artifact title with its associated hues.": """
                SELECT m.title, c.hue
               FROM metadata m
               INNER JOIN colors c ON m.id = c.objid;
            """,

            
             "18.Get artifact titles, cultures, and media ranks where the period is not null.": """
                SELECT m.title, m.culture, md.rank
               FROM metadata m
               INNER JOIN media md ON m.id = md.objid
               WHERE m.period IS NOT NULL;
            """,

            
             '19.Find artifact titles ranked is greater than 10 that include the color hue "Grey"': """
                SELECT distinct m.title,md.rank
               FROM metadata m
               INNER JOIN media md ON m.id = md.objid
               INNER JOIN colors c ON m.id = c.objid
               WHERE md.`rank` >= 10
                AND c.hue = 'Grey';
            """,

            
             "20.How many artifacts exist per classification, and what is the average media count for each?": """
                SELECT m.classification, count(*) AS total_artifacts,
               AVG(md.mediacount) AS avg_media
               FROM metadata m
               INNER JOIN media md on m.id = md.objid
               GROUP BY m.classification;
            """,

            
             "21.List all artifacts and their associated media counts (if available), including those artifacts that don’t have any media.": """
                SELECT m.title, md.mediacount
               FROM metadata m
               LEFT JOIN media md ON m.id = md.objid;
            """,

            
             "22.List artifact titles and hues, including artifacts that have no color information.": """
                SELECT m.title, c.hue
               FROM metadata m
               LEFT JOIN colors c ON m.id = c.objid;
            """,

            
              "23.List all colors and their associated artifact titles, even if some colors aren’t linked to any artifact.": """
                SELECT c.color, m.title
               FROM colors c
               LEFT JOIN metadata m ON c.objid = m.id;
            """,

            
             "24.List all media entries and the corresponding artifact titles, even if some media records aren’t linked to any artifact.": """
                SELECT md.*, m.title
               FROM media md
               LEFT JOIN metadata m ON md.objid = m.id;
            """,

            
             "25.List all artifacts (metadata) along with their media counts (from media table).": """
                SELECT m.title, md.mediacount
               FROM metadata m
               LEFT JOIN media md ON m.id = md.objid;
            """
        }
        selected_query = st.selectbox("Choose Query", list(queries.keys()))
        if st.button("Run Query"):
            df = run_query(queries[selected_query])
            st.write("DEBUG:", df) 
            st.dataframe(df)

if __name__ == "__main__":
    main()
