--
-- PostgreSQL database dump
--

-- Dumped from database version 14.2
-- Dumped by pg_dump version 14.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: arke_field; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.arke_field (
    id character varying(255) NOT NULL,
    label character varying(255) NOT NULL,
    type character varying(255) DEFAULT 'string'::character varying NOT NULL,
    format character varying(255) DEFAULT 'attribute'::character varying NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    is_primary boolean DEFAULT false NOT NULL
);


--
-- Name: arke_link; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.arke_link (
    type character varying(255) DEFAULT 'link'::character varying NOT NULL,
    parent_id character varying(255) NOT NULL,
    child_id character varying(255) NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: arke_schema; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.arke_schema (
    id character varying(255) NOT NULL,
    label character varying(255) NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    active boolean DEFAULT true NOT NULL,
    inserted_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    type character varying(255) DEFAULT 'arke'::character varying NOT NULL
);


--
-- Name: arke_schema_field; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.arke_schema_field (
    arke_schema_id character varying(255) NOT NULL,
    arke_field_id character varying(255) NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: arke_unit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.arke_unit (
    id character varying(255) NOT NULL,
    arke_id character varying(255) NOT NULL,
    data jsonb DEFAULT '{}'::jsonb NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp(0) without time zone NOT NULL,
    updated_at timestamp(0) without time zone NOT NULL
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version bigint NOT NULL,
    inserted_at timestamp(0) without time zone
);


--
-- Name: arke_field arke_field_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_field
    ADD CONSTRAINT arke_field_pkey PRIMARY KEY (id);


--
-- Name: arke_link arke_link_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_link
    ADD CONSTRAINT arke_link_pkey PRIMARY KEY (parent_id, child_id, metadata);


--
-- Name: arke_schema_field arke_schema_field_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_schema_field
    ADD CONSTRAINT arke_schema_field_pkey PRIMARY KEY (arke_schema_id, arke_field_id);


--
-- Name: arke_schema arke_schema_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_schema
    ADD CONSTRAINT arke_schema_pkey PRIMARY KEY (id);


--
-- Name: arke_unit arke_unit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_unit
    ADD CONSTRAINT arke_unit_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: arke_link_child_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX arke_link_child_id_index ON public.arke_link USING btree (child_id);


--
-- Name: arke_link_metadata_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX arke_link_metadata_index ON public.arke_link USING btree (metadata);


--
-- Name: arke_link_parent_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX arke_link_parent_id_index ON public.arke_link USING btree (parent_id);


--
-- Name: arke_schema_field_arke_field_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX arke_schema_field_arke_field_id_index ON public.arke_schema_field USING btree (arke_field_id);


--
-- Name: arke_schema_field_arke_schema_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX arke_schema_field_arke_schema_id_index ON public.arke_schema_field USING btree (arke_schema_id);


--
-- Name: arke_link arke_link_child_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_link
    ADD CONSTRAINT arke_link_child_id_fkey FOREIGN KEY (child_id) REFERENCES public.arke_unit(id);


--
-- Name: arke_link arke_link_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_link
    ADD CONSTRAINT arke_link_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.arke_unit(id);


--
-- Name: arke_schema_field arke_schema_field_arke_field_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_schema_field
    ADD CONSTRAINT arke_schema_field_arke_field_id_fkey FOREIGN KEY (arke_field_id) REFERENCES public.arke_field(id);


--
-- Name: arke_schema_field arke_schema_field_arke_schema_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.arke_schema_field
    ADD CONSTRAINT arke_schema_field_arke_schema_id_fkey FOREIGN KEY (arke_schema_id) REFERENCES public.arke_schema(id);


--
-- PostgreSQL database dump complete
--

INSERT INTO public."schema_migrations" (version) VALUES (20211110141127);
INSERT INTO public."schema_migrations" (version) VALUES (20211110142203);
INSERT INTO public."schema_migrations" (version) VALUES (20211110142330);
INSERT INTO public."schema_migrations" (version) VALUES (20211125090958);
INSERT INTO public."schema_migrations" (version) VALUES (20211125091252);
INSERT INTO public."schema_migrations" (version) VALUES (20211125091704);
INSERT INTO public."schema_migrations" (version) VALUES (20211129152516);
