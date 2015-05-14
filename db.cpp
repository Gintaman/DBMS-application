/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
    rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) && ((cur->next->tok_value == K_INTO))){
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if((cur->tok_value == K_SELECT)) {
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if((cur->tok_value == K_UPDATE)) {
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if((cur->tok_value == K_DELETE) && (cur->next->tok_value == K_FROM)) {
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else if((cur->tok_value == K_UPDATE)) {
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else
  	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case INSERT:
						rc = sem_insert(cur);
						break;			
			case SELECT:
						rc = sem_select(cur);
						break;
			case DELETE:
						rc = sem_delete(cur);
						break;
			case UPDATE:
						rc = sem_update(cur);
						break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];

	table_file_header *header;
	header = (table_file_header*) malloc(sizeof(table_file_header));
	int record_size = 0;
	FILE *fhandle;

	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);

										record_size += (1 + 4); 
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);

											record_size += (1 + col_entry[cur_id].col_len);

											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						while(record_size % 4 != 0) { //round to 4 bytes
							record_size++;
						}
						memset(header, '\0', sizeof(table_file_header));
						header->file_size = sizeof(table_file_header);
						printf("%d\n", header->file_size);
						
						header->record_size = record_size; //fixed size for each row
						header->record_offset = sizeof(table_file_header);
						header->num_records = 0;
						char filename[MAX_IDENT_LEN+1+4]; //+4 for ".tab"
						strcpy(filename, new_entry->table_name);	
						strcat(filename, ".tab");


						if((fhandle = fopen(filename, "wbc")) == NULL) {
							rc = FILE_OPEN_ERROR;
						} 
						else {
							fwrite(header, sizeof(table_file_header), 1, fhandle);
							fclose(fhandle);
						}

						fhandle = fopen(filename, "rbc");
						table_file_header *header2 = (table_file_header*) malloc(sizeof(table_file_header));

						//test
						fread(header2, sizeof(table_file_header), 1, fhandle);
						fclose(fhandle);
						printf("File size: %d\nRecord size: %d\n Num records: %d\n Record offset: %d\n",
							header2->file_size, header2->record_size, header2->num_records, header2->record_offset);

						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	FILE *fhandle = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				strcpy(filename, cur->tok_string);
				strcat(filename, ".tab");
				remove(filename);
				printf("Dropped table %s\n", filename);
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert(token_list *t_list) 
{
	int rc = 0;
	token_list *cur;

	tpd_entry *tab_entry; //get table entry
	cd_entry *col_entry;
	cd_entry *cur_col;
	int num_entries = 0;
	int record_size = 0;
	table_file_header *header = (table_file_header*) malloc(sizeof(table_file_header));
	
	cur = t_list;
	
	token_list *values; //points to start of values >> VALUES ( ... )

	FILE *fhandle = NULL;

	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1 + 4]; //+4 for ".tab"

	
	if((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)) {
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else {
		if((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			printf("Table does not exist\n");
		}
		else { //table exists
			//insert into [current token]
			header->tpd_ptr = tab_entry;
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			strcpy(filename, cur->tok_string);
			strcat(filename, ".tab");

			if((fhandle = fopen(filename, "rbc")) == NULL) {
				rc = FILE_OPEN_ERROR;
			}
			else {
				fread(header, sizeof(table_file_header), 1, fhandle);
				fclose(fhandle);
			}
			
			//insert into students [current token]
			cur = cur->next;
			if(cur->tok_value != K_VALUES) {
				rc = MISSING_VALUES_KEYWORD;
				cur->tok_value = INVALID;
				printf("Missing keyword 'VALUES'\n");
			}
			else {
				//INSERT INTO students VALUES [current token]
				cur = cur->next;
				if(cur->tok_value != S_LEFT_PAREN) {
					rc = MISSING_OPEN_PAREN;
					cur->tok_value = INVALID;
					printf("Missing opening parens\n");
				}
				else {
					cur = cur->next;
					//INSERT INTO students VALUES ( [current token]
					if(cur->tok_value == S_RIGHT_PAREN) {
						rc = EMPTY_VALUES;
						cur->tok_value = INVALID;
						printf("Missing values\n");
					}
					if(cur->tok_value == EOC) {//maybe just if
						rc = MISSING_CLOSING_PAREN;
						cur->tok_value = INVALID;
						printf("Expected closing paren\n"); //maybe change to invalid syntax
					}
					if(!rc) {
						col_entry = (cd_entry*) ((char*)tab_entry + tab_entry->cd_offset);
						cur_col = col_entry;
						//values_ptr = cur;
					}

					values = cur; //pointer to values
					//printf("values: %s\n", values->tok_string);
					do {
						if(cur->tok_value == K_NULL || cur->tok_value == STRING_LITERAL || cur->tok_value == INT_LITERAL) {
							num_entries++;
							cur = cur->next;
						}
						//insert into students values ([current token]
						else if(cur->tok_value == S_COMMA) { //consume comma
							cur = cur->next;
						}
						else if(cur->tok_value == EOC) {
							rc = END_OF_INPUT;
							cur->tok_value = INVALID;
							printf("Syntax error, unexpected end of input\n");
						}
						else {
							rc = INVALID_SYNTAX;
							cur->tok_value = INVALID;
							printf("Invalid syntax\n");
						}
					}while((rc==0) && (cur->tok_value != S_RIGHT_PAREN)); //find a closing paren, finish parsing values
					//INSERT INTO students VALUES ('jeff', 12 [current_token]
					
					if(!rc) {
						//INSERT INTO students VALUES ('jeff', 12)
						
						if((fhandle = fopen(filename, "ab+c")) == NULL) {
							rc = FILE_OPEN_ERROR;
						} 
						else {
							//printf("valid insert\n");
							//printf("%d\n", num_entries);
							//printf("%s\n", col_entry->col_name);
							//printf("%s, %d\n", values->tok_string, values->tok_value);
							//printf("record size: %d\n", header->record_size);
							char *buffer = (char*) malloc(header->record_size);
							memset(buffer, '\0', header->record_size);

							int offset = 0;
							
							if(cur->tok_value == S_RIGHT_PAREN) {
								if(num_entries == tab_entry->num_columns) {
									int i;
									int len = 1;
									for(i = 0; i < num_entries; i++) {
										if(!rc) {
											if(values->tok_value == K_NULL) {
												if(cur_col->not_null) {
													rc = CANNOT_BE_NULL;
													values->tok_value = INVALID;
													printf("Value cannot be NULL\n");
												}
											}
											else if(values->tok_value == STRING_LITERAL) {
												if(cur_col->col_type != T_CHAR) {
													rc = INVALID_DATA_TYPE;
													values->tok_value = INVALID;
													printf("Expected a string\n");
												}
												else if(strlen(values->tok_string) > cur_col->col_len) {
													rc = STRING_OVERFLOW;
													values->tok_value = INVALID;
													printf("String is too long\n");
												}

											}
											else if(values->tok_value == INT_LITERAL) {
												if(cur_col->col_type != T_INT) {
													rc = INVALID_DATA_TYPE;
													values->tok_value = INVALID;
													printf("Expected an integer\n");
												}
											}
											else {
												rc = INVALID_DATA_TYPE;
												values->tok_value = INVALID;
												printf("Values can be only be strings or integers\n");
											}
										} //end checking for valid values
										//INSERT INTO students VALUES ('jeff', 12, 'hello')
										int numval;
										int nullval = 0;
										if(!rc) {
											//insert here
											if(values->tok_value == K_NULL) {
												memcpy(buffer + offset, &nullval, 1); //set length byte to 0 for null
											}
											else {
												memcpy(buffer + offset, (&len), 1); //1 otherwise
											}
											
											offset++;
											if(values->tok_value == STRING_LITERAL) {
												memcpy(buffer + offset, values->tok_string, cur_col->col_len);
											}
											else {
												numval = atoi(values->tok_string);
												memcpy(buffer + offset, (&numval), cur_col->col_len);
											}
											
											offset += cur_col->col_len;
											//printf("col info: %s, len: %d\n", cur_col->col_name, cur_col->col_len);
											values = values->next->next; //consume comma, go to next column entry
											cur_col++; //move to next cd entry
											
										}
									} //end for loop iterating over values
									//INSERT INTO students VALUES('jeff', 12, 'hello') [current token]
									
									header->file_size = header->file_size + header->record_size;
									//header->record_offset += header->record_size;
									header->num_records++;
									cur = cur->next; 
									cur->tok_value = values->tok_value; //NOT SURE


									fhandle = fopen(filename, "abc");
									fwrite(buffer, header->record_size, 1, fhandle); //append record
									fclose(fhandle);
									fhandle = fopen(filename, "r+bc"); //open for write, set at beginning of file
									fwrite(header, sizeof(table_file_header), 1, fhandle); //update header
								}
								else {
									rc = INVALID_COLUMN_LENGTH;
									cur->tok_value = INVALID;
									printf("Number of values and number of columns do not match\n");
								}
							}
							else {
								rc = MISSING_CLOSING_PAREN;
								cur->tok_value = INVALID;
								printf("Missing closing paren\n");
							}
							if(!rc) {
								if(cur->tok_value != EOC) {
									rc = INVALID_STATEMENT;
									cur->tok_value = INVALID;
									printf("Syntax error, invalid statement\n");
								}
							}
						}
					}//parse insert
				} //finish parsing values
			}//missing keyword values
		} //table exists, valid table
	}
	return rc;
}

//bookmark
int sem_select(token_list *t_list) {
	int rc = 0;
	token_list *cur;

	tpd_entry *tab_entry; //get table entry
	cd_entry *col_entry;
	cd_entry *temp;

	int num_records = 0;
	int record_size = 0;

	int column_constraint = 0;

	table_file_header *header = (table_file_header*) malloc(sizeof(table_file_header));
	
	cur = t_list;
	
	FILE *fhandle = NULL;

	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1 + 4]; //+4 for ".tab"
	memset(tab_name, '\0', MAX_IDENT_LEN + 1);
	memset(filename, '\0', MAX_IDENT_LEN + 1 + 4);

	if(cur->tok_value == S_STAR) { //SELECT *
		column_constraint = S_STAR;
	}
	else if (cur->tok_value == IDENT) { //SELECT [column name]
		column_constraint = IDENT;
	}
	else {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		printf("Invalid statement\n");
	}
	if(!rc) {
		cur = cur->next; //SELECT [* || column] [current token]
		if(cur->tok_value != K_FROM) { //SELECT * [invalid token]
			rc = INVALID_SYNTAX;
			cur->tok_value = INVALID;
			printf("Missing keyword FROM\n");
		}
		else { //SELECT [* || column] FROM
			cur = cur->next;
			//SELECT [* || column] FROM [table name]
			if((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
				printf("Table does not exist\n");
			}
			else {
				col_entry = (cd_entry*) ((char*)tab_entry + tab_entry->cd_offset); //column info
				//SELECT * FROM [current token]
				strcpy(tab_name, cur->tok_string);
				strcpy(filename, cur->tok_string);
				strcat(filename, ".tab");
				if((fhandle = fopen(filename, "rbc")) == NULL) {
					printf("File error\n");
					rc = FILE_OPEN_ERROR;
				}
				else {
					fread(header, sizeof(table_file_header), 1, fhandle); //get table file header
					
					num_records = header->num_records;
					record_size = header->record_size;
				}
			}
			if(!rc) {
				if(column_constraint == S_STAR) {
					int i = 0; //initial position of pointer, skip the first length byte
					int j = 0;
					int len = 0;
					int k;
					int l;
					char *buffer;
					
					//fread(buffer, header->record_size, 1, fhandle); //copied row to buffer
					temp = col_entry;
					for(k = 0; k < tab_entry->num_columns; k++) {
						if((temp->col_len) > (strlen(temp->col_name))) {
							printf("%s", temp->col_name);
							for(l = 0; l < ((temp->col_len) - (strlen(temp->col_name))); l++) {
								printf(" ");
							}
						}
						else {
							printf("%s ", temp->col_name);
						}
						temp++;
					}
					printf("\n");

					int val;
					char *stringbuffer; //actual result buffer
					char *lengthbuffer = (char *) malloc((sizeof(char) * 1) + 1);
					memset(lengthbuffer, '\0', 2);
					int padding = 0;

					int maxlength = 0;
					for(j = 0; j < header->num_records; j++) {
						temp = col_entry;
						padding = 0;
						for(i = 0; i < tab_entry->num_columns; i++) {
							stringbuffer = (char *) malloc((sizeof(char) * temp->col_len) + 1);
							memset(stringbuffer, '\0', temp->col_len + 1);
							memset(lengthbuffer, '\0', 2);
							if(temp->col_type == T_CHAR) { //if its a string
								fread(lengthbuffer, 1, 1, fhandle); //flush first length byte
								if(lengthbuffer[0] == '0' || lengthbuffer[0] == 0) {
									fread(stringbuffer, temp->col_len, 1, fhandle);
									printf("-", stringbuffer);
									if(temp->col_len > strlen(temp->col_name)) { //for formatting
										int formatbuffer = temp->col_len - strlen(stringbuffer);
										for(k = 0; k < formatbuffer - 1; k++) {
											printf(" ");
										}
									}
									else {
										int formatbuffer = strlen(temp->col_name) - strlen(stringbuffer);
										for(k = 0; k < formatbuffer; k++) {
											printf(" ");
										}
									}	
								}
								else {
									 //set buffer back to '\0'
									fread(stringbuffer, temp->col_len, 1, fhandle);
									printf("%s", stringbuffer);
									if(temp->col_len > strlen(temp->col_name)) { //for formatting
										int formatbuffer = temp->col_len - strlen(stringbuffer);
										for(k = 0; k < formatbuffer; k++) {
											printf(" ");
										}
									}
									else {
										int formatbuffer = strlen(temp->col_name) - strlen(stringbuffer);
										for(k = 0; k <= formatbuffer; k++) {
											printf(" ");
										}
									}	
								}
								
							}
							else {
								int max_width = strlen(temp->col_name);
								fread(lengthbuffer, 1, 1, fhandle); //flush first length byte
								if(lengthbuffer[0] == '0' || lengthbuffer[0] == 0) {
									memset(stringbuffer, '\0', temp->col_len + 1);
									fread(&val, sizeof(int), 1, fhandle);	
									char dash = '-';
									printf("%*c ", max_width, dash);
								}
								else {
									memset(stringbuffer, '\0', temp->col_len + 1);
									fread(&val, sizeof(int), 1, fhandle);	
									printf("%*d ", max_width, val);
								}
								//bookmark
							}
							padding += (temp->col_len) + 1;
							temp++;
						}
						stringbuffer = (char *) malloc((sizeof(char)) * 2);
						while(padding % 4 != 0) {
							fread(stringbuffer, 1, 1, fhandle);
							padding++;
						}
						printf("\n");
					}
					fclose(fhandle);


				}
				else {
					printf("select column\n");
				}
			}
			
		}//end select [* || column] from
	}


	return rc;
}

int sem_delete(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	return rc;
}

int sem_update(token_list *t_list) {
	int rc = 0;
	int i, num_columns;
	token_list *cur = t_list;
	token_list *update_ptr;
	token_list *where_val;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	cd_entry *col_ptr = NULL;
	table_file_header *header = (table_file_header *) malloc(sizeof(table_file_header));
	FILE *fhandle = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	char update_column[MAX_IDENT_LEN+1];
	char op[5];
	memset(tab_name, '\0', MAX_IDENT_LEN+1);
	memset(filename, '\0', MAX_IDENT_LEN+1);
	memset(update_column, '\0', MAX_IDENT_LEN+1);
	memset(op, '\0', 5);
	int offset = 0;
	int column_found = 0;


	//update table set column = [] where []

	if((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)) {
		rc = INVALID_TABLE_NAME;
		printf("Invalid table name\n");
	} 
	else {
		if((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			printf("Table does not exist\n");
		} 
		else {
			col_entry = (cd_entry *) ((char *) tab_entry + tab_entry->cd_offset);
			col_ptr = col_entry; //keep a pointer to first column entry
			strcpy(tab_name, cur->tok_string);
			strcpy(filename, cur->tok_string);
			strcat(filename, ".tab");
			if((fhandle = fopen(filename, "r+bc")) == NULL) {
				rc = FILE_OPEN_ERROR;
				printf("File error\n");
			}
			else {
				fread(header, sizeof(table_file_header), 1, fhandle);
				cur = cur-> next; //update table [set] 
				if(cur->tok_value != K_SET) {
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					printf("Missing SET keyword\n");
				}
				else {
					cur = cur->next; //update table set [column name]
					num_columns = tab_entry->num_columns;
					strcpy(update_column, cur->tok_string);
					for(i = 0; i < num_columns; i++) { //search for matching column
						if(stricmp(col_entry->col_name, update_column) == 0) {
							column_found = 1;
							break;
						}  
						offset += col_entry->col_len + 1; //add length of column + 1 for length byte
						col_entry++;
					}
					if(!column_found) {
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
						printf("Column not found\n");
					}
					else {
						cur = cur->next; //update class set student [=]
						if(cur->tok_value != S_EQUAL) {
							rc = INVALID_SYNTAX;
							cur->tok_value = INVALID;
							printf("Missing '='\n");
						}
						else {
							cur = cur->next; //update class set student = [user value]
							update_ptr = cur;
							//check for column type
							if(cur->tok_value == K_NULL && col_entry->not_null) {
								rc = CANNOT_BE_NULL;
								cur->tok_value = INVALID;
								printf("Column is not null\n");
							}
							else if(cur->tok_value == STRING_LITERAL) {//update class set student = 'string'
								if(col_entry->col_type != T_CHAR) {
									rc = INVALID_DATA_TYPE;
									cur->tok_value = INVALID;
									printf("Invalid data type\n");
								}
							}
							else if(cur->tok_value == INT_LITERAL) {
								if(col_entry->col_type != T_INT) {
									rc = INVALID_DATA_TYPE;
									cur->tok_value = INVALID;
									printf("Invalid data type\n");
								}
							}
						}
						cur = cur->next; //update class set student = 'string' [WHERE]
						if(cur->tok_value != K_WHERE) {
							rc = INVALID_SYNTAX;
							cur->tok_value = INVALID;
							printf("Missing WHERE keyword\n");
						}
						else {
							cur = cur->next; //update class set student = 'jeff' where [id]
							col_entry = col_ptr;
							column_found = 0;
							for(i = 0; i < tab_entry->num_columns; i++) {
								if(stricmp(cur->tok_string, col_entry->col_name) == 0) {
									column_found = 1;
									break;
								}
								offset += col_entry->col_len + 1;
								col_entry++;
							}
							if(!column_found) {
								rc = INVALID_COLUMN_NAME;
								cur->tok_value = INVALID;
								printf("Column not found\n");
							}
							else {
								cur = cur->next;
								if((cur->tok_value != S_EQUAL) && (cur->tok_value != S_LESS) && (cur->tok_value != S_GREATER)) {
									printf("%d\n", cur->tok_value);
									rc = INVALID_SYNTAX;
									cur->tok_value = INVALID;
									printf("Invalid syntax\n");
								}
								else {
									strcpy(op, cur->tok_string); //update class set student = 'jeff' where id [op]
									//printf("op: %s\n", cur->tok_string);
									cur = cur->next; //update class set student = jeff where id = 
									where_val = cur;
									if(cur->tok_value == NULL && col_entry->not_null) {
										rc = CANNOT_BE_NULL;
										cur->tok_value = INVALID;
										printf("Column cannot be null\n");
									}
									else if(cur->tok_value == STRING_LITERAL) {
										if(col_entry->col_type != T_CHAR) {
											rc = INVALID_DATA_TYPE;
											cur->tok_value = INVALID;
											printf("Invalid data type\n");
										}
									}
									else if(cur->tok_value == INT_LITERAL) {
										if(col_entry->col_type != T_INT) {
											rc = INVALID_DATA_TYPE;
											cur->tok_value = INVALID;
											printf("Invalid data type\n");
										}
									}
									else {
										cur = cur->next;
										if(cur->tok_value != EOC) {
											rc = INVALID_STATEMENT;
											cur->tok_value = INVALID;
											printf("Invalid statement\n");
										}
									}
								}
							}
						}
					}
				}
			}
			
		}
		if(!rc) {
			printf("update memory here\n");
		}
	}
	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}


int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}