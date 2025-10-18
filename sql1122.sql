CREATE database project_1;

use project_1;

select * from colors;
select * from metadata;
select * from media;
2.What are the unique cultures represented in the artifacts?
select DISTINCT culture from metadata;

3.List all artifacts from the Archaic Period.
select * from metadata where period = 'Archaic Period';

4.List artifact titles ordered by accession year in descending order
SELECT title, accessionyear from metadata
where accessionyear is not null 
order by accessionyear DESC;

5.How many artifacts are there per department?
select department, count(*) as artifact_total
from metadata 
group by department
order by artifact_total;

1.List all artifacts from the 11th century belonging to Byzantine culture.
select century,culture from metadata 
where culture = 'Byzantine' and century = '11th century';


6.Which artifacts have more than 1 image?
select * from media
where imagecount > 1;

7.What is the average rank of all artifacts?
select avg(`rank`) as average_rank
from media;

8.Which artifacts have a higher colorcount than mediacount?
SELECT * FROM MEDIA
WHERE colorcount > mediacount;

9.List all artifacts created between 1500 and 1600.
select * from media 
where datebegin >= 1500 and dateend <= 1600;

10.How many artifacts have no media files?
select count(*) as no_media
from media
where mediacount = 0;

11.What are all the distinct hues used in the dataset?
select distinct hue 
from colors;

12.What are the top 5 most used colors by frequency?
select color, count(*) as frequency
from colors
group by color
order by frequency desc 
limit 5;

13.What is the average coverage percentage for each hue?
select hue, avg(ercent) as coverage_average
from colors
group by hue;
 
 
14.List all colors used for a given artifact ID.
select * from colors
where objid = 280069;

15.What is the total number of color entries in the dataset?
select count(*) as total_entries
from colors;


16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.
SELECT m.title, c.hue
FROM metadata m
INNER JOIN colors c ON m.id = c.objid
WHERE m.culture = 'Byzantine';

17.List each artifact title with its associated hues.
SELECT m.title, c.hue
FROM metadata m
INNER JOIN colors c ON m.id = c.objid;

18.Get artifact titles, cultures, and media ranks where the period is not null.
SELECT m.title, m.culture, md.rank
FROM metadata m
INNER JOIN media md ON m.id = md.objid
WHERE m.period IS NOT NULL;

19.Find artifact titles ranked is greater than 10 that include the color hue "Grey"
SELECT distinct m.title,md.rank
FROM metadata m
INNER JOIN media md ON m.id = md.objid
INNER JOIN colors c ON m.id = c.objid
WHERE md.`rank` >= 10
  AND c.hue = 'Grey';


20.How many artifacts exist per classification, and what is the average media count for each?
SELECT m.classification, count(*) AS total_artifacts,
AVG(md.mediacount) AS avg_media
FROM metadata m
INNER JOIN media md on m.id = md.objid
GROUP BY m.classification;

21.List all artifacts and their associated media counts (if available), including those artifacts that don’t have any media.
SElECT m.title, md.mediacount
FROM metadata m
LEFT JOIN media md ON m.id = md.objid;

22.List artifact titles and hues, including artifacts that have no color information.
SELECT m.title, c.hue
FROM metadata m
LEFT JOIN colors c ON m.id = c.objid;

23.List all colors and their associated artifact titles, even if some colors aren’t linked to any artifact.
SELECT c.color, m.title
FROM colors c
LEFT JOIN metadata m ON c.objid = m.id;

24.List all media entries and the corresponding artifact titles, even if some media records aren’t linked to any artifact.
SELECT md.*, m.title
FROM media md
LEFT JOIN metadata m ON md.objid = m.id;

25.List all artifacts (metadata) along with their media counts (from media table).
SELECT m.title, md.mediacount
FROM metadata m
LEFT JOIN media md ON m.id = md.objid;