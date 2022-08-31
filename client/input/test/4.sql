-- to check var collide
-- collide zero 0, four 4
-- collide L23 L_23


select '${MACRO_s4a}'||'${MACRO_s4b}', ${MACRO_i4a} + ${MACRO_i4b}, 
${MACRO_i4c}, ${NOT_EXIST_MACRO} from my_project.my_dataset.${MACRO_1} my;
