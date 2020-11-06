-- SELECT question.id,COUNT(DISTINCT(submission_id)) cc
-- FROm survey.response
-- JOIN survey.question ON response.question_id=question.id
-- GROUP BY question.id
-- ORDER BY cc DESC;

-- SELECT COUNT(DISTINCT(question.id))
-- FROm survey.response
-- JOIN survey.question ON response.question_id=question.id;


-- SELECT COUNT(DISTINCT (survey_id))
-- FROM survey.response;

-- SELECT *
-- FROm survey.response
-- JOIN survey.question ON response.question_id=question.id
-- WHERE question.id='acae7b1e-273d-4965-b71a-339c68a2f0f9';

SELECT a.survey_id, b.idd , a_count, b.survey_title, b.b_count, b.maxdate
FROM
(SELECT survey_id, COUNT(DISTINCT (submission_id)) a_count
FROM survey.response
GROUP BY 1) a
LEFT JOIN (
SELECT survey.id idd, survey.survey_title,COUNT(DISTINCT (submission_id)) b_count, MAX(response.created_at) maxdate
FROM survey.response
JOIN survey.question ON response.question_id=question.id
JOIN survey.survey On survey.id=response.survey_id
GROUP BY 1,2) b ON a.survey_id=b.idd
ORDER BY a_count DESC;

-- SELECT question_id
-- FROM survey.response
-- WHERE survey_id='55f21054-36c4-4939-a63c-de01dc4dfee9' AND (question_id::Text='5260ea29-4329-4da8-8091-8aa05fd62ff7' OR question_id::Text='3fcc8ec3-65f1-4b36-b793-a8eebf4e6e0b')
-- GROUP BY question_id;

-- SELECT question_description, display_text
-- FROM survey.question
-- WHERE display_text  LIKE '%"%' AND id IN (
--  SELECT question.id
--  FROm survey.response
--  JOIN survey.question ON response.question_id=question.id
--  WHERE response.survey_id::Text='55f21054-36c4-4939-a63c-de01dc4dfee9' OR response.survey_id::Text='cbfa78b5-6bb3-4af0-a301-a7b2c3fdd209' OR
-- 	  response.survey_id::Text='fec88521-d60c-40bc-89f4-f390797baba1' OR response.survey_id::Text='be0e17a5-aeb2-466a-8395-5da1db0fb8a5' OR
-- 	  response.survey_id::Text='ec576a06-4b7d-4507-a02b-4025d2177c30' OR response.survey_id::Text='f07de7b5-c6cd-42c5-985a-f762b01fd720'
--  GROUP BY question.id
--  HAVING COUNT(DISTINCT(submission_id))>50
)






