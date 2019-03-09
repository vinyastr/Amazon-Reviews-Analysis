reviews = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/mergedfile' AS(marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date);

grpd = GROUP reviews BY product_id;

cntd = FOREACH grpd GENERATE group, COUNT(reviews);

result = FILTER cntd by $1 > 10000;

STORE result INTO '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/sentimentAnalysisdata/tempData/new3';

--reviews = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/mergedfile' AS(marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date);

out = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/sentimentAnalysisdata/tempData/new3/out';

result1 = join reviews by product_id, out by $0;

final = FOREACH result1 GENERATE $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14;

STORE final INTO '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/sentimentAnalysisdata/Indata3';