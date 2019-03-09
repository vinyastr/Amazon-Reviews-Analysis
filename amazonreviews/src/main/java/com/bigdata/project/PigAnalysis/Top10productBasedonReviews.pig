bookreviews = LOAD '/Users/vinyaskaushiktr/Downloads/mergedfiltereddata' AS(marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date);

filtered = FILTER bookreviews BY star_rating == 5;

grpd = GROUP filtered BY product_id;

cntd = FOREACH grpd GENERATE group, COUNT(filtered);

sorted = ORDER cntd BY $1  DESC;

top10 = LIMIT sorted 10;

STORE top10 INTO '/Users/vinyaskaushiktr/Downloads/top10MostReviewedBooks';


